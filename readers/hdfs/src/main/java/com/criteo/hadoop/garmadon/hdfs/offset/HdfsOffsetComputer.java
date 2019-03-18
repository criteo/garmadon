package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.reader.Offset;
import org.apache.hadoop.fs.*;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Persist and read offset based on HDFS file names, named _date_/_partition_._offset_
 */
public class HdfsOffsetComputer implements OffsetComputer {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsOffsetComputer.class);
    private final Pattern offsetFilePatternGenerator;
    private final FileSystem fs;
    private final Path basePath;
    private int backlogDays;
    private final String fileRenamePattern;
    private final String dirRenamePattern;

    public HdfsOffsetComputer(FileSystem fs, Path basePath, int backlogDays) {
        this(fs, basePath, null, backlogDays);
    }

    /**
     * @param fs           Filesystem for said files
     * @param basePath     Root directory to look in for offsets filenames
     * @param kafkaCluster Name of corresponding kafka cluster
     * @param backlogDays  How many days to look for offsets for in the past (earlier directories will be ignored),
     *                     eg. 2 =&lt; today and tomorrow
     */
    public HdfsOffsetComputer(FileSystem fs, Path basePath, @Nullable String kafkaCluster, int backlogDays) {
        this.fs = fs;
        this.basePath = basePath;
        this.backlogDays = backlogDays;
        this.dirRenamePattern = "%s";

        if (kafkaCluster == null) {
            this.offsetFilePatternGenerator = Pattern.compile("^(?<partitionId>\\d+)(?>\\.index=(?<index>\\d+))*.*$");
            this.fileRenamePattern = "%d.index=%d";
        } else {
            this.offsetFilePatternGenerator = Pattern.compile(String.format("^(?<partitionId>\\d+)\\.cluster=%s(?>\\.index=(?<index>\\d+))*.*$", kafkaCluster));
            this.fileRenamePattern = String.format("%%d.cluster=%s.index=%%d", kafkaCluster);
        }
    }

    @Override
    public Map<Integer, Long> computeOffsets(Collection<Integer> partitionIds) throws IOException {
        Set<Integer> partitionIdsSet = new HashSet<>(partitionIds);

        final Map<Integer, Long> result = partitionIds.stream().collect(Collectors.toMap(Function.identity(), i -> NO_OFFSET));
        final Map<Integer, Map<String, FinalEventPartitionFile>> resultFile = new HashMap<>();

        LocalDateTime today = LocalDateTime.now();
        for (int i = 0; i < backlogDays; ++i) {
            LocalDateTime day = today.minusDays(i);
            String listedDay = day.format(DateTimeFormatter.ISO_DATE);

            String dirsPattern = String.format("%s/*", computeDirName(day));
            FileStatus[] fileStatuses = fs.globStatus(new Path(basePath, dirsPattern));

            for (FileStatus status : fileStatuses) {
                String fileName = status.getPath().getName();
                Matcher matcher = offsetFilePatternGenerator.matcher(fileName);
                if (matcher.matches()) {
                    try {
                        int partitionId = Integer.parseInt(matcher.group("partitionId"));
                        Long index = Long.parseLong(matcher.group("index"));
                        if (partitionIdsSet.contains(partitionId) &&
                                (!resultFile.containsKey(partitionId) || !resultFile.get(partitionId).containsKey(listedDay) ||
                                        index > resultFile.get(partitionId).get(listedDay).getIndex())) {
                            HashMap<String, FinalEventPartitionFile> dateFinalEventPartitionFile =
                                    (HashMap<String, FinalEventPartitionFile>) resultFile.computeIfAbsent(partitionId, s -> new HashMap<>());
                            dateFinalEventPartitionFile.put(listedDay,
                                    new FinalEventPartitionFile(index, status.getPath()));
                            resultFile.put(partitionId, dateFinalEventPartitionFile);
                        }
                    } catch (NumberFormatException e) {
                        LOGGER.info("Couldn't deviate a valid offset from '{}'", fileName);
                    }
                }
            }
        }

        // Get last offset
        for (int partitionId : resultFile.keySet()) {
            result.put(partitionId, getMaxOffset(resultFile.get(partitionId)));
        }

        return result;
    }

    protected Long getMaxOffset(Map<String, FinalEventPartitionFile> dateFinalEventPartitionFile) throws IOException {
        List<Long> maxOffsets = new ArrayList<>();
        for (FinalEventPartitionFile finalEventPartitionFile : dateFinalEventPartitionFile.values()) {
            // Read parquet file
            ParquetFileReader pFR = ParquetFileReader.open(fs.getConf(), finalEventPartitionFile.getFilePath());

            // Get max value from all blocks
            long maxValue = pFR.getFooter().getBlocks().stream()
                    .map(b -> {
                        Optional<ColumnChunkMetaData> columnChunkMetaData1 = b.getColumns().stream()
                                .filter(column -> Arrays.stream(column.getPath().toArray()).allMatch(path -> path.equals("kafka_offset")))
                                .findFirst();

                        if (columnChunkMetaData1.isPresent()) {
                            return ((LongStatistics) columnChunkMetaData1.get().getStatistics()).genericGetMax();
                        } else {
                            return NO_OFFSET;
                        }

                    })
                    .reduce(NO_OFFSET, (max1, max2) -> max1 > max2 ? max1 : max2);

            maxOffsets.add(maxValue);
        }

        // Get max offset from all files for a partition
        return maxOffsets.stream().reduce(NO_OFFSET, (max1, max2) -> max1 > max2 ? max1 : max2);
    }

    private String computeDirName(LocalDateTime time) {
        return String.format(dirRenamePattern, time.format(DateTimeFormatter.ISO_DATE));
    }

    @Override
    public long getIndex(String fileName) {
        Matcher matcher = offsetFilePatternGenerator.matcher(fileName);
        if (matcher.matches()) {
            try {
                return Long.parseLong(matcher.group("index"));
            } catch (NumberFormatException e) {
                LOGGER.info("Couldn't deviate a valid index from '{}'", fileName);
            }
        }
        return 0L;
    }

    @Override
    public String computeTopicGlob(LocalDateTime time, Offset offset) {
        return computeDirName(time) + "/" + offset.getPartition() + ".*";
    }

    @Override
    public String computePath(LocalDateTime time, long index, Offset offset) {
        return computeDirName(time) + "/" + String.format(fileRenamePattern, offset.getPartition(), index);
    }

    protected class FinalEventPartitionFile {
        private long index;
        private Path filePath;

        FinalEventPartitionFile(long index, Path filePath) {
            this.index = index;
            this.filePath = filePath;
        }

        long getIndex() {
            return index;
        }

        Path getFilePath() {
            return filePath;
        }
    }
}
