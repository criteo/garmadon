package com.criteo.hadoop.garmadon.hdfs.offset;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

/**
 * Write checkpoints on the filesystem.
 */
public class FsBasedCheckpointer implements Checkpointer {
    private static final Logger LOGGER = LoggerFactory.getLogger(FsBasedCheckpointer.class.getName());

    private final FileSystem fs;
    private final LoadingCache<Path, Boolean> checkpointsCache;
    private BiFunction<Integer, Instant, Path> checkpointPathGenerator;

    /**
     * @param fs                        The FS on which checkpoints are persisted
     * @param checkpointPathGenerator   Generate a path from a date.
     */
    public FsBasedCheckpointer(FileSystem fs, BiFunction<Integer, Instant, Path> checkpointPathGenerator) {
        this.fs = fs;
        this.checkpointPathGenerator = checkpointPathGenerator;
        this.checkpointsCache = CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<Path, Boolean>() {
            @ParametersAreNonnullByDefault
            @Override
            public Boolean load(Path path) throws Exception {
                return fs.exists(path);
            }
        });
    }

    @Override
    public boolean tryCheckpoint(int partitionId, Instant when) {
        Path path = checkpointPathGenerator.apply(partitionId, when);

        try {
            if (!checkpointed(path)) {
                checkpoint(path);
                checkpointsCache.refresh(path);

                return true;
            }
        } catch (IOException | ExecutionException e) {
            LOGGER.warn("Couldn't write checkpoint file: {}", e.getMessage());
        }

        return false;
    }

    private void checkpoint(Path path) throws IOException {
        FSDataOutputStream os = fs.create(path);

        os.close();
    }

    private boolean checkpointed(Path path) throws ExecutionException {
        return checkpointsCache.get(path);
    }
}
