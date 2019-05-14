package com.criteo.hadoop.garmadon.hdfs.offset;

import java.time.Instant;

public interface Checkpointer {
    /**
     * Try writing a checkpoint.
     *
     * @param partitionId   Which date this checkpoint is about.
     * @param when          Which partition this checkpoint is about.
     * @return              True if the checkpoint was created, false otherwise.
     */
    boolean tryCheckpoint(int partitionId, Instant when);
}
