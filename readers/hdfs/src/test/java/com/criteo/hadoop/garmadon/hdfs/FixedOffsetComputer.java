package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Always return the same file name when asked for it
 */
public class FixedOffsetComputer implements OffsetComputer {
    private final String fileName;
    private final long offset;

    /**
     * @param fileName  The name to return everytime
     */
    public FixedOffsetComputer(String fileName, long offset) {
        this.fileName = fileName;
        this.offset = offset;
    }

    @Override
    public Map<Integer, Long> computeOffsets(Collection<Integer> partitionIds) {
        HashMap<Integer, Long> result = new HashMap<>();

        for (int partitionId: partitionIds) {
            result.put(partitionId, offset);
        }

        return result;
    }

    @Override
    public long getIndex(String fileName) {
        return 0;
    }

    @Override
    public String computeTopicGlob(LocalDateTime time, int partition) {
        return fileName + "*";
    }

    @Override
    public String computePath(LocalDateTime time, long index, int partition) {
        return fileName;
    }
}
