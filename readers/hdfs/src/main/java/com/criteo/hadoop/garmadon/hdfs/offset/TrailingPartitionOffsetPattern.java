package com.criteo.hadoop.garmadon.hdfs.offset;

import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Provide a pattern to match files named _partition_._offset_
 */
public class TrailingPartitionOffsetPattern implements Function<Integer, Pattern> {
    @Override
    public Pattern apply(final Integer partitionId) {
        return Pattern.compile(String.format("^%d.(\\d+)$", partitionId));
    }
}
