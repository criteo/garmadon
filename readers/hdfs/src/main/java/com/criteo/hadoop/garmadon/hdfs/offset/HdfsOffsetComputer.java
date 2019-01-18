package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.reader.Offset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Persist and read offset based on HDFS file names, named _date_/_partition_._offset_
 */
public class HdfsOffsetComputer implements OffsetComputer {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsOffsetComputer.class);
    private final Function<Integer, Pattern> offsetFilePatternGenerator;
    private final FileSystem fs;
    private final Path basePath;

    /**
     * @param fs                            Filesystem for said files
     * @param basePath                      Root directory to look in for offsets filenames
     */
    public HdfsOffsetComputer(FileSystem fs, Path basePath) {
        this.fs = fs;
        this.basePath = basePath;
        this.offsetFilePatternGenerator = partitionId -> Pattern.compile(String.format("^%d.(\\d+)$", partitionId));
    }

    @Override
    public Map<Integer, Long> computeOffsets(Collection<Integer> partitionIds) throws IOException {
        final RemoteIterator<LocatedFileStatus> filesIterator = fs.listFiles(basePath, true);
        final Collection<String> fileNames = new ArrayList<>();
        final Map<Integer, Long> result = new HashMap<>();

        while (filesIterator.hasNext()) {
            fileNames.add(filesIterator.next().getPath().getName());
        }

        for (int partitionId: partitionIds) {
            result.put(partitionId, computeOffset(partitionId, fileNames));
        }

        return result;
    }

    private long computeOffset(int partitionId, Collection<String> fileNames) {
        final Pattern partitionBasedPattern = offsetFilePatternGenerator.apply(partitionId);
        long highestOffset = NO_OFFSET;

        for (String fileName: fileNames) {
            final Matcher matcher = partitionBasedPattern.matcher(fileName);
            if (!matcher.matches() || matcher.groupCount() < 1) continue;

            final String offsetString = matcher.group(1);
            final long offset;

            try {
                 offset = Long.valueOf(offsetString);
            } catch (NumberFormatException e) {
                LOGGER.info("Couldn't deviate a valid offset from '{}'", offsetString);
                continue;
            }

            if (offset > highestOffset) {
                highestOffset = offset;
            }
        }

        return highestOffset;
    }

    @Override
    public String computePath(LocalDateTime time, Offset offset) {
        return String.format("%s/%d.%d", time.format(DateTimeFormatter.ISO_DATE), offset.getPartition(),
                offset.getOffset());
    }
}
