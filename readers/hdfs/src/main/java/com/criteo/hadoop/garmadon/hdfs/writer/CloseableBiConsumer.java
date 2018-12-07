package com.criteo.hadoop.garmadon.hdfs.writer;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Combination of Closeable and Consumer<T, U>
 * @param <T>
 * @param <U>
 */
interface CloseableBiConsumer<T, U> {
    void write(T t, U u) throws IOException;

    /**
     * Close the underlying medium.
     *
     * @return              Closed file path
     * @throws IOException  If failing to close
     */
    Path close() throws IOException;
}

