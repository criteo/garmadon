package com.criteo.hadoop.garmadon.hdfs.writer;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

interface CloseableBiConsumer<T, U> {
    void write(T t, U u) throws IOException;
    Path close() throws IOException;
}

