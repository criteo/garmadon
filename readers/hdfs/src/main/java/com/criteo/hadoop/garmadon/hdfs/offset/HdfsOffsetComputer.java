package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.reader.Offset;
import org.apache.hadoop.fs.*;
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
     * @param fs            Filesystem for said files
     * @param basePath      Root directory to look in for offsets filenames
     * @param kafkaCluster  Name of corresponding kafka cluster
     * @param backlogDays   How many days to look for offsets for in the past (earlier directories will be ignored),
     *                      eg. 2 =&lt; today and tomorrow
     */
    public HdfsOffsetComputer(FileSystem fs, Path basePath, @Nullable String kafkaCluster, int backlogDays) {
        this.fs = fs;
        this.basePath = basePath;
        this.backlogDays = backlogDays;
        this.dirRenamePattern = "%s";

        if (kafkaCluster == null) {
            this.offsetFilePatternGenerator = Pattern.compile("^(\\d+)\\.(\\d+)$");
            this.fileRenamePattern = "%d.%d";
        } else {
            this.offsetFilePatternGenerator = Pattern.compile(String.format("^(\\d+)\\.cluster=%s\\.(\\d+)$", kafkaCluster));
            this.fileRenamePattern = String.format("%%d.cluster=%s.%%d", kafkaCluster);
        }
    }

    @Override
    public Map<Integer, Long> computeOffsets(Collection<Integer> partitionIds) throws IOException {
        Set<Integer> partitionIdsSet = new HashSet<>(partitionIds);
        final Map<Integer, Long> result = partitionIds.stream().collect(Collectors.toMap(Function.identity(), i -> NO_OFFSET));
        String dirsPattern = String.format("%s/**", computeDirNamesPattern(LocalDateTime.now(), backlogDays));
        FileStatus[] fileStatuses = fs.globStatus(new Path(basePath, dirsPattern));

        for (FileStatus status: fileStatuses) {
            String fileName = status.getPath().getName();
            Matcher matcher = offsetFilePatternGenerator.matcher(fileName);
            if (matcher.matches()) {
                try {
                    int partitionId = Integer.parseInt(matcher.group(1));
                    Long offset = Long.parseLong(matcher.group(2));
                    if (partitionIdsSet.contains(partitionId)) {
                        result.merge(partitionId, offset, Long::max);
                    }
                } catch (NumberFormatException e) {
                    LOGGER.info("Couldn't deviate a valid offset from '{}'", fileName);
                }
            }
        }

        return result;
    }

    private String computeDirNamesPattern(LocalDateTime today, int backlogDays) {
        List<String> dirnames = new ArrayList<>(backlogDays);

        for (int i = 0; i < backlogDays; ++i) {
            LocalDateTime day = today.minusDays(i);

            dirnames.add(computeDirName(day));
        }

        return "{" + String.join(",", dirnames) + "}";
    }

    private String computeDirName(LocalDateTime time) {
        return String.format(dirRenamePattern, time.format(DateTimeFormatter.ISO_DATE));
    }

    @Override
    public String computePath(LocalDateTime time, Offset offset) {
        return computeDirName(time) + "/" + String.format(fileRenamePattern, offset.getPartition(), offset.getOffset());
    }
}
