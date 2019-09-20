package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.reader.Offset;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Writer that can be closed
 * @param <M> the message type to write
 */
interface CloseableWriter<M> {
    void write(long timestamp, M t, Offset offset) throws IOException;

    /**
     * Close the underlying medium.
     *
     * @return              Closed file path
     * @throws IOException  If failing to close
     */
    Path close() throws IOException, SQLException;
}

