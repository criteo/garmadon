package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.hdfs.FixedFileNamer;
import com.criteo.hadoop.garmadon.reader.TopicPartitionOffset;
import com.google.protobuf.Message;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class ParquetWithOffsetWriterTest {
    private static final String FINAL_FILE_NAME = "finalFile";
    private static final LocalDateTime UTC_EPOCH = LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC"));
    private static final String TOPIC = "topic";

    @Test
    public void closeWithSomeEvents() throws IOException {
        ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        Path tmpPath = new Path("tmp");
        Path finalPath = new Path("final");
        FileSystem fsMock = mock(FileSystem.class);
        Message firstMessageMock = mock(Message.class);
        Message secondMessageMock = mock(Message.class);
        ParquetWithOffsetWriter consumer = new ParquetWithOffsetWriter<>(writerMock, tmpPath,
                finalPath, fsMock, new FixedFileNamer(FINAL_FILE_NAME), LocalDateTime.MIN);

        consumer.write(firstMessageMock, new TopicPartitionOffset(TOPIC, 1, 2));
        consumer.write(secondMessageMock, new TopicPartitionOffset(TOPIC, 1, 3));
        verify(writerMock, times(1)).write(firstMessageMock);
        verify(writerMock, times(1)).write(secondMessageMock);
        verifyNoMoreInteractions(writerMock);

        consumer.close();

        verify(fsMock, times(1)).rename(tmpPath, new Path(finalPath, FINAL_FILE_NAME));
        verifyNoMoreInteractions(fsMock);
    }

    @Test
    public void closeWithNoEvent() throws IOException {
        ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        ParquetWithOffsetWriter parquetWriter = new ParquetWithOffsetWriter<>(writerMock, new Path("tmp"),
                new Path("final"), null, null, LocalDateTime.MIN);

        parquetWriter.close();
        verifyZeroInteractions(writerMock);
    }

    // We want to check that an empty file gets created and therefore need an actual FS
    @Test
    public void closeWithNullEventWithLocalFilesystem() throws IOException {
        Collection<EventHeaderProtos.Header> headers = checkSingleFileWithFileSystem(Collections.singleton(null));

        Assert.assertEquals(0, headers.size());
    }

    @Test
    public void closeAfterSomeEventWithLocalFilesystem() throws IOException {
        List<EventHeaderProtos.Header> inputHeaders = new LinkedList<>();

        inputHeaders.add(EventHeaderProtos.Header.newBuilder().setAppAttemptId("1").build());
        inputHeaders.add(EventHeaderProtos.Header.newBuilder().setAppAttemptId("2").build());

        List<EventHeaderProtos.Header> headers = checkSingleFileWithFileSystem(inputHeaders);

        Assert.assertEquals(2, headers.size());
        Assert.assertEquals("1", headers.get(0).getAppAttemptId());
        Assert.assertEquals("2", headers.get(1).getAppAttemptId());
    }

    private List<EventHeaderProtos.Header> checkSingleFileWithFileSystem(
            Collection<EventHeaderProtos.Header> inputHeaders) throws IOException {
        java.nio.file.Path tmpDir = Files.createTempDirectory("hdfs-reader-test-");
        List<EventHeaderProtos.Header> headers = new LinkedList<>();

        try {
            Path baseDir = new Path(tmpDir.toString());
            Path tmpPath = new Path(baseDir, "tmp");
            long offset = 1;
            FileSystem localFs = FileSystem.getLocal(new Configuration());
            ProtoParquetWriter<Message> writer = new ProtoParquetWriter<>(tmpPath, EventHeaderProtos.Header.class);

            ParquetWithOffsetWriter consumer = new ParquetWithOffsetWriter<>(writer, tmpPath, baseDir, localFs,
                    new FixedFileNamer(FINAL_FILE_NAME), UTC_EPOCH);

            for (EventHeaderProtos.Header header : inputHeaders) {
                consumer.write(header, new TopicPartitionOffset(TOPIC, 1, offset++));
            }
            consumer.close();

            RemoteIterator<LocatedFileStatus> filesIterator = localFs.listFiles(baseDir, false);
            LocatedFileStatus fileStatus = filesIterator.next();
            Assert.assertEquals(FINAL_FILE_NAME, fileStatus.getPath().getName());
            Assert.assertFalse("There should be only one output file", filesIterator.hasNext());

            ParquetReader<EventHeaderProtos.Header.Builder> reader;
            reader = ProtoParquetReader.<EventHeaderProtos.Header.Builder>builder(fileStatus.getPath()).build();

            EventHeaderProtos.Header.Builder current = reader.read();
            while (current != null) {
                headers.add(current.build());
                current = reader.read();
            }

            return headers;
        }
        finally {
            FileUtils.deleteDirectory(tmpDir.toFile());
        }
    }
}
