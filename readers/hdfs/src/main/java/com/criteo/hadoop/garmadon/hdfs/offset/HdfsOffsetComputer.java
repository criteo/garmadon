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

/**
 * Compute starting offset based on HDFS file names.
 */
public class HdfsOffsetComputer implements OffsetComputer {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsOffsetComputer.class);
    private final Function<Integer, Pattern> offsetFilePatternGenerator;
    private final FileSystem fs;
    private final Path basePath;

    /**
     * @param fs                            Filesystem for said files
     * @param basePath                      Rooth directory to look in for offsets filenames
     * @param offsetFilePatternGenerator    First capturing group must capture offset
     */
    public HdfsOffsetComputer(FileSystem fs, Path basePath, Function<Integer, Pattern> offsetFilePatternGenerator) {
        this.fs = fs;
        this.basePath = basePath;
        this.offsetFilePatternGenerator = offsetFilePatternGenerator;
    }

    @Override
    public long compute(int partitionId) throws IOException {
        final RemoteIterator<LocatedFileStatus> filesIterator = fs.listFiles(basePath, true);
        final Pattern partitionBasedPattern = offsetFilePatternGenerator.apply(partitionId);
        long highestOffset = NO_OFFSET;

        while (filesIterator.hasNext()) {
            final LocatedFileStatus fileStatus = filesIterator.next();
            final String fileName = fileStatus.getPath().getName();

            final Matcher matcher = partitionBasedPattern.matcher(fileName);
            if (!matcher.matches() || matcher.groupCount() < 1)
                continue;

            final String offsetString = matcher.group(1);
            final long offset;

            try {
                 offset = Long.valueOf(offsetString);
            }
            catch (NumberFormatException e) {
                LOGGER.info("Couldn't deviate a valid offset from '{}'", offsetString);
                continue;
            }

            if (offset > highestOffset) {
                highestOffset = offset;
            }
        }

        return highestOffset;
    }
}
