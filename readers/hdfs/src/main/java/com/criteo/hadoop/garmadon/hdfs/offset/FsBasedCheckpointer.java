package com.criteo.hadoop.garmadon.hdfs.offset;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Write checkpoints on the filesystem.
 */
public class FsBasedCheckpointer implements Checkpointer {
    private static final Logger LOGGER = LoggerFactory.getLogger(FsBasedCheckpointer.class.getName());

    private final FileSystem fs;
    private final Map<Path, Boolean> checkpointsCache;
    private Function<Instant, Path> checkpointPathGenerator;

    /**
     * @param fs                        The FS on which checkpoints are persisted
     * @param checkpointPathGenerator   Generate a path from a date.
     */
    public FsBasedCheckpointer(FileSystem fs, Function<Instant, Path> checkpointPathGenerator) {
        this.fs = fs;
        this.checkpointPathGenerator = checkpointPathGenerator;
        this.checkpointsCache = new HashMap<>();
    }

    @Override
    public boolean tryCheckpoint(Instant when) {
        Path path = checkpointPathGenerator.apply(when);

        try {
            if (!checkpointed(path)) {
                checkpoint(path);
                checkpointsCache.put(path, true);

                return true;
            } else {
                checkpointsCache.put(path, true);
            }
        } catch (IOException e) {
            LOGGER.warn("Couldn't write checkpoint file: {}", e.getMessage());
        }

        return false;
    }

    private void checkpoint(Path path) throws IOException {
        FSDataOutputStream os = fs.create(path);

        os.close();
    }

    private boolean checkpointed(Path path) throws IOException {
        if (!checkpointsCache.containsKey(path)) {
            checkpointsCache.put(path, fs.exists(path));
        }

        return checkpointsCache.get(path);
    }
}
