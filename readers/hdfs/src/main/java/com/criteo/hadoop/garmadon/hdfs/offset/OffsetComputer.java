package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.reader.Offset;

import java.io.IOException;
import java.time.LocalDateTime;

public interface OffsetComputer {
    long NO_OFFSET = -1;

    /**
     * Compute the max offset for a given partition.
     *
     * @param partitionId   The partition to computeOffset the offset for
     * @return              NO_OFFSET if no offset could be fetched but there was no error
     * @throws IOException  If there was an issue while fetching the offset
     */
    long computeOffset(int partitionId) throws IOException;

    /**
     * @param time      Time-window start time (eg. day start if daily)
     * @param offset    Kafka offset
     * @return          Path based on a time and offset
     */
    String computePath(LocalDateTime time, Offset offset);
}
