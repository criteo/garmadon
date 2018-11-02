package com.criteo.hadoop.garmadon.hdfs.offset;

import java.io.IOException;

public interface OffsetComputer {
    long NO_OFFSET = -1;

    /**
     * Compute the max offset for a given partition.
     *
     * @param partitionId   The partition to compute the offset for
     * @return              NO_OFFSET if no offset could be fetched but there was no error
     * @throws IOException  If there was an issue while fetching the offset
     */
    long compute(int partitionId) throws IOException;
}
