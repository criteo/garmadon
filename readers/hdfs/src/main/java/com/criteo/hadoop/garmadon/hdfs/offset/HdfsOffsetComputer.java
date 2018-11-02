package com.criteo.hadoop.garmadon.hdfs.offset;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HdfsOffsetComputer implements OffsetComputer {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsOffsetComputer.class);
    private final Function<Integer, Pattern> offsetFilePatternGenerator;
    private FileSystem fs;
    private Path basePath;

    /**
     *
     * @param fs
     * @param basePath
     * @param offsetFilePatternGenerator    First capturing group must capture offset
     */
    public HdfsOffsetComputer(FileSystem fs, Path basePath, Function<Integer, Pattern> offsetFilePatternGenerator) {
        this.fs = fs;
        this.basePath = basePath;
        this.offsetFilePatternGenerator = offsetFilePatternGenerator;
    }

    @Override
    public long compute(int partitionId) throws IOException {
        RemoteIterator<LocatedFileStatus> filesIterator = fs.listFiles(basePath, true);
        Pattern partitionBasedPattern = offsetFilePatternGenerator.apply(partitionId);
        long highestOffset = NO_OFFSET;

        while (filesIterator.hasNext()) {
            LocatedFileStatus fileStatus = filesIterator.next();
            String fileName = fileStatus.getPath().getName();

            Matcher matcher = partitionBasedPattern.matcher(fileName);
            if (!matcher.matches() || matcher.groupCount() < 1)
                continue;

            String offsetString = matcher.group(1);
            long offset;

            try {
                 offset = Long.valueOf(offsetString);
            }
            catch (NumberFormatException e) {
                LOGGER.info(String.format("Couldn't deviate a valid offset from '%s'", offsetString));
                continue;
            }

            if (offset > highestOffset) {
                highestOffset = offset;
            }
        }

        return highestOffset;
    }
}
