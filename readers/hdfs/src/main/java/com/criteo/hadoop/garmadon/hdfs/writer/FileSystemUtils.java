package com.criteo.hadoop.garmadon.hdfs.writer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;

public final class FileSystemUtils {
    private FileSystemUtils() {
    }

    /**
     * Ensure paths exist and are directories. Otherwise create them.
     *
     * @param dirs          Directories that need to exist
     * @param fs            Filesystem to which these directories should belong
     * @throws IOException  When failing to create any of the directories
     */
    public static void ensureDirectoriesExist(Collection<Path> dirs, FileSystem fs) throws IOException {
        for (Path dir : dirs) {
            if (!fs.exists(dir) && !fs.mkdirs(dir)) {
                throw new IOException(String.format("Couldn't create %s (no specific reason)", dir.toUri()));
            }
        }
    }
}
