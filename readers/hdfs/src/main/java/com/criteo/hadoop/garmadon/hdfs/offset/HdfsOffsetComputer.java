package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.reader.Offset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
    private final String kafkaCluster;

    public HdfsOffsetComputer(FileSystem fs, Path basePath) {
        this(fs, basePath, null);
    }

    /**
     * @param fs                            Filesystem for said files
     * @param basePath                      Root directory to look in for offsets filenames
     * @param kafkaCluster                  Name of corresponding kafka cluster
     */
    public HdfsOffsetComputer(FileSystem fs, Path basePath, @Nullable String kafkaCluster) {
        this.fs = fs;
        this.basePath = basePath;
        this.kafkaCluster = kafkaCluster;
        if (kafkaCluster == null) {
            this.offsetFilePatternGenerator = Pattern.compile("^(\\d+)\\.(\\d+)$");
        } else {
            this.offsetFilePatternGenerator = Pattern.compile(String.format("^(\\d+)\\.cluster=%s\\.(\\d+)$", kafkaCluster));
        }
    }

    @Override
    public Map<Integer, Long> computeOffsets(Collection<Integer> partitionIds) throws IOException {
        Set<Integer> partitionIdsSet = new HashSet<>(partitionIds);
        final RemoteIterator<LocatedFileStatus> filesIterator = fs.listFiles(basePath, true);
        final Map<Integer, Long> result = partitionIds.stream().collect(Collectors.toMap(Function.identity(), i -> NO_OFFSET));

        while (filesIterator.hasNext()) {
            String fileName = filesIterator.next().getPath().getName();
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

    @Override
    public String computePath(LocalDateTime time, Offset offset) {
        if (kafkaCluster == null) {
            return String.format("%s/%d.%d", time.format(DateTimeFormatter.ISO_DATE), offset.getPartition(),
                    offset.getOffset());
        } else {
            return String.format("%s/%d.cluster=%s.%d", time.format(DateTimeFormatter.ISO_DATE), offset.getPartition(),
                    kafkaCluster, offset.getOffset());
        }
    }
}
