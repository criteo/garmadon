package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.hdfs.hive.HiveClient;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;

/**
 * Wrap an actual ProtoParquetWriter, renaming the output file properly when closing.
 *
 * @param <MESSAGE_KIND> The message to be written in Proto + Parquet
 */
public class HiveProtoParquetWriterWithOffset<MESSAGE_KIND extends MessageOrBuilder> implements CloseableWriter<MESSAGE_KIND> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveProtoParquetWriterWithOffset.class);

    private ProtoParquetWriterWithOffset protoParquetWriterWithOffset;
    private HiveClient hiveClient;


    /**
     * @param protoParquetWriterWithOffset The actual Proto + Parquet writer
     * @param hiveClient                   The hive client
     */
    public HiveProtoParquetWriterWithOffset(ProtoParquetWriterWithOffset<MESSAGE_KIND> protoParquetWriterWithOffset, HiveClient hiveClient) {
        this.hiveClient = hiveClient;
        this.protoParquetWriterWithOffset = protoParquetWriterWithOffset;
    }

    @Override
    public void write(long timestamp, MESSAGE_KIND msg, Offset offset) throws IOException {
        protoParquetWriterWithOffset.write(timestamp, msg, offset);
    }

    @Override
    public Path close() throws IOException, SQLException {
        // We need the writer to be closed to be able to read its metadata
        protoParquetWriterWithOffset.closeWriter();

        hiveClient.createPartitionIfNotExist(protoParquetWriterWithOffset.getEventName(),
            protoParquetWriterWithOffset.getWriter().getFooter().getFileMetaData().getSchema(),
            protoParquetWriterWithOffset.getDayStartTime().format(DateTimeFormatter.ISO_DATE), protoParquetWriterWithOffset.getFinalHdfsDir().toString());
        return protoParquetWriterWithOffset.close();
    }

    public CloseableWriter<MESSAGE_KIND> withHiveSupport(boolean isHiveSuppport) {
        if (isHiveSuppport) {
            return this;
        } else {
            return protoParquetWriterWithOffset;
        }
    }
}
