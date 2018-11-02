package com.criteo.hadoop.garmadon.hdfs.offset;

import java.util.function.Function;
import java.util.regex.Pattern;

public class TrailingPartitionOffsetPattern implements Function<Integer, Pattern> {
    @Override
    public Pattern apply(Integer partitionId) {
        return Pattern.compile(String.format("^%d.(\\d+)$", partitionId));
    }
}
