package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.hdfs.hive.HiveClient;
import com.google.protobuf.Message;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashMap;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class HiveProtoParquetWriterWithOffsetTest {
    private ProtoParquetWriterWithOffset<Message> protoParquetWriterWithOffset;

    private HiveClient hiveClient;

    private MessageType schema;
    private String eventName = "fs";
    private Path finalPath = new Path("final");

    @Before
    public void setup() throws IOException {
        protoParquetWriterWithOffset = mock(ProtoParquetWriterWithOffset.class);
        hiveClient = mock(HiveClient.class);

        when(protoParquetWriterWithOffset.getEventName()).thenReturn(eventName);
        when(protoParquetWriterWithOffset.getFinalHdfsDir()).thenReturn(finalPath);
        ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        when(protoParquetWriterWithOffset.getWriter()).thenReturn(writerMock);
        ParquetMetadata parquetMetadata = mock(ParquetMetadata.class);
        when(writerMock.getFooter()).thenReturn(parquetMetadata);

        PrimitiveType appId = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "app_id");
        schema = new MessageType("fs", appId);
        FileMetaData fileMetaData = new FileMetaData(schema, new HashMap<String, String>(), "test");
        when(parquetMetadata.getFileMetaData()).thenReturn(fileMetaData);

        when(protoParquetWriterWithOffset.getDayStartTime()).thenReturn(LocalDateTime.of(2019, 9, 10, 10, 10, 10));
    }

    @Test
    public void returnHiveProtoParquetWriterWithOffset() {
        CloseableHdfsWriter parquetWriter = new HiveProtoParquetWriterWithOffset<>(protoParquetWriterWithOffset, hiveClient).withHiveSupport(true);
        assertTrue(parquetWriter instanceof HiveProtoParquetWriterWithOffset);
    }

    @Test
    public void returnProtoParquetWriterWithOffset() {
        CloseableHdfsWriter parquetWriter = new HiveProtoParquetWriterWithOffset<>(protoParquetWriterWithOffset, hiveClient).withHiveSupport(false);
        assertTrue(parquetWriter instanceof ProtoParquetWriterWithOffset);
    }

    @Test
    public void callCreatePartitionIfNotExistOnClose() throws IOException, SQLException {
        CloseableHdfsWriter parquetWriter = new HiveProtoParquetWriterWithOffset<>(protoParquetWriterWithOffset, hiveClient).withHiveSupport(true);
        parquetWriter.close();
        verify(hiveClient, times(1)).createPartitionIfNotExist(eq(eventName), eq(schema), anyString(), eq(finalPath.toString()));
    }

    @Test(expected = IOException.class)
    public void throwExceptionIfProtoParquetWriterWithOffsetFailed() throws IOException, SQLException {
        doThrow(new IOException("Failure")).when(protoParquetWriterWithOffset).close();
        CloseableHdfsWriter parquetWriter = new HiveProtoParquetWriterWithOffset<>(protoParquetWriterWithOffset, hiveClient).withHiveSupport(true);
        parquetWriter.close();
    }

    @Test(expected = SQLException.class)
    public void throwExceptionIfHiveClientFailed() throws IOException, SQLException {
        doThrow(new SQLException("Failure")).when(hiveClient).createPartitionIfNotExist(eq(eventName), eq(schema), anyString(), eq(finalPath.toString()));
        CloseableHdfsWriter parquetWriter = new HiveProtoParquetWriterWithOffset<>(protoParquetWriterWithOffset, hiveClient).withHiveSupport(true);
        parquetWriter.close();
    }

}
