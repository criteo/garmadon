package com.criteo.hadoop.garmadon.hdfs.offset;

public class FixedOffsetComputer implements OffsetComputer {
    private long offset;

    /**
     * Always return the same offset.
     *
     * @param offset    The offset to return everytime
     */
    public FixedOffsetComputer(long offset) {
        this.offset = offset;
    }

    @Override
    public long compute(int partitionId) {
        return offset;
    }
}
