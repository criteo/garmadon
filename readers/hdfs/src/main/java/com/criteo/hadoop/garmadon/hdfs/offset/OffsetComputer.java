package com.criteo.hadoop.garmadon.hdfs.offset;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;

public interface OffsetComputer {
    long NO_OFFSET = -1;

    /**
     * Compute the max offset for a given partition.
     *
     * @param partitionIds  The partitions to computeOffset the offsets for
     * @return              A partition id =&gt; offset map. Value is NO_OFFSET if no offset could be fetched but there was
     *                      no error
     * @throws IOException  If there was an issue while fetching the offset
     */
    Map<Integer, Long> computeOffsets(Collection<Integer> partitionIds) throws IOException;

    /**
     * @param fileName  A final fileName from which we want to find index
     * @return          Index of the file
     */
    long getIndex(String fileName);

    /**
     * @param time      Time-window start time (eg. day start if daily)
     * @return          Path based on time
     */
    String computeDirName(LocalDateTime time);

    /**
     * @param time      Time-window start time (eg. day start if daily)
     * @param partition Kafka partition
     * @return          Topic Glob Path based on a time and offset
     */
    String computeTopicGlob(LocalDateTime time, int partition);

    /**
     * @param time      Time-window start time (eg. day start if daily)
     * @param index     file index
     * @param partition Kafka partition
     * @return          Path based on a time and offset
     */
    String computePath(LocalDateTime time, long index, int partition);
}
