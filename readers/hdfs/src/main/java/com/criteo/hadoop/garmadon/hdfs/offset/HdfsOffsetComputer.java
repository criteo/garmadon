package com.criteo.hadoop.garmadon.hdfs.offset;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
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
        this.dirRenamePattern = "day=%s";

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
                        long index = Long.parseLong(matcher.group("index"));
                        if (isPartitionComputedByThisReader(partitionId, partitionIdsSet) &&
                            checkCurrentFileIndexBiggerTheOneInHashPartitionDay(partitionId, listedDay, index, resultFile)) {
                            HashMap<String, FinalEventPartitionFile> dateFinalEventPartitionFile =
                                (HashMap<String, FinalEventPartitionFile>) resultFile.computeIfAbsent(partitionId, HashMap::new);
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
        for (Map.Entry<Integer, Map<String, FinalEventPartitionFile>> partitionIdDateFile : resultFile.entrySet()) {
            result.put(partitionIdDateFile.getKey(), getMaxOffset(partitionIdDateFile.getValue()));
        }

        return result;
    }

    private boolean isPartitionComputedByThisReader(int partitionId, Set<Integer> partitionIdsSet) {
        return partitionIdsSet.contains(partitionId);
    }

    private boolean checkCurrentFileIndexBiggerTheOneInHashPartitionDay(int partitionId, String listedDay, long index, Map<Integer,
        Map<String, FinalEventPartitionFile>> resultFile) {
        return isPartitionDayNotAlreadyInHashPartitionDay(partitionId, listedDay, resultFile) || index > resultFile.get(partitionId).get(listedDay).getIndex();
    }

    private boolean isPartitionDayNotAlreadyInHashPartitionDay(int partitionId, String listedDay, Map<Integer,
        Map<String, FinalEventPartitionFile>> resultFile) {
        return !(resultFile.containsKey(partitionId) && resultFile.get(partitionId).containsKey(listedDay));
    }

    protected Long getMaxOffset(Map<String, FinalEventPartitionFile> dateFinalEventPartitionFile) {
        // Get max offset from all files for a partition
        return dateFinalEventPartitionFile
            .values()
            .stream()
            .flatMap(finalEventPartitionFile -> {
                try (ParquetFileReader pFR = ParquetFileReader.open(fs.getConf(), finalEventPartitionFile.getFilePath())) {
                    return pFR.getFooter().getBlocks().stream();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            })
            .map(b -> b.getColumns().stream()
                .filter(column -> Arrays.stream(column.getPath().toArray()).allMatch(path -> path.equals("kafka_offset")))
                .findFirst()
                .map(ColumnChunkMetaData::getStatistics)
                .map(Statistics::genericGetMax)
                .map(Long.class::cast)
                .orElse(NO_OFFSET))
            .mapToLong(Long::longValue)
            .max()
            .orElse(NO_OFFSET);
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
    public String computeTopicGlob(LocalDateTime time, int partition) {
        return computeDirName(time) + "/" + partition + ".*";
    }

    @Override
    public String computePath(LocalDateTime time, long index, int partition) {
        return computeDirName(time) + "/" + String.format(fileRenamePattern, partition, index);
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
