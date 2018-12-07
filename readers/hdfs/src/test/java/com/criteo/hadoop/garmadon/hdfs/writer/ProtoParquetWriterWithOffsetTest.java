package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.hdfs.FixedOffsetComputer;
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
public class ProtoParquetWriterWithOffsetTest {
    private static final String FINAL_FILE_NAME = "finalFile";
    private static final LocalDateTime UTC_EPOCH = LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC"));
    private static final String TOPIC = "topic";

    @Test
    public void closeWithSomeEvents() throws IOException {
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final Path tmpPath = new Path("tmp");
        final Path finalPath = new Path("final");
        final FileSystem fsMock = mock(FileSystem.class);
        final Message firstMessageMock = mock(Message.class);
        final Message secondMessageMock = mock(Message.class);
        final ProtoParquetWriterWithOffset consumer = new ProtoParquetWriterWithOffset<>(writerMock, tmpPath,
                finalPath, fsMock, new FixedOffsetComputer(FINAL_FILE_NAME, 123), LocalDateTime.MIN);

        consumer.write(firstMessageMock, new TopicPartitionOffset(TOPIC, 1, 2));
        consumer.write(secondMessageMock, new TopicPartitionOffset(TOPIC, 1, 3));
        verify(writerMock, times(1)).write(firstMessageMock);
        verify(writerMock, times(1)).write(secondMessageMock);
        verifyNoMoreInteractions(writerMock);

        // Directory doesn't exist and creation succeeds
        when(fsMock.exists(any(Path.class))).thenReturn(false);
        when(fsMock.mkdirs(any(Path.class))).thenReturn(true);

        when(fsMock.rename(any(Path.class), any(Path.class))).thenReturn(true);

        consumer.close();

        verify(fsMock, times(1)).rename(tmpPath, new Path(finalPath, FINAL_FILE_NAME));
        verify(fsMock, times(1)).exists(eq(finalPath));
        verify(fsMock, times(1)).mkdirs(eq(finalPath));
        verifyNoMoreInteractions(fsMock);
    }

    @Test(expected = IOException.class)
    public void closeWithNoEvent() throws IOException {
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final ProtoParquetWriterWithOffset parquetWriter = new ProtoParquetWriterWithOffset<>(writerMock,
                new Path("tmp"), new Path("final"), null, null, LocalDateTime.MIN);

        parquetWriter.close();
    }

    @Test(expected = IOException.class)
    public void closeRenameFails() throws IOException {
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final FileSystem fsMock = mock(FileSystem.class);
        final ProtoParquetWriterWithOffset parquetWriter = new ProtoParquetWriterWithOffset<>(writerMock,
                new Path("tmp"), new Path("final"), fsMock, null, LocalDateTime.MIN);

        when(fsMock.rename(any(Path.class), any(Path.class))).thenReturn(false);
        parquetWriter.close();
    }

    // We want to check that an empty file gets created and therefore need an actual FS
    @Test
    public void closeWithNullEventWithLocalFilesystem() throws IOException {
        final Collection<EventHeaderProtos.Header> headers = checkSingleFileWithFileSystem(Collections.singleton(null));

        Assert.assertEquals(0, headers.size());
    }

    @Test
    public void closeAfterSomeEventWithLocalFilesystem() throws IOException {
        final List<EventHeaderProtos.Header> inputHeaders = new LinkedList<>();

        inputHeaders.add(EventHeaderProtos.Header.newBuilder().setAttemptId("1").build());
        inputHeaders.add(EventHeaderProtos.Header.newBuilder().setAttemptId("2").build());

        final List<EventHeaderProtos.Header> headers = checkSingleFileWithFileSystem(inputHeaders);

        Assert.assertEquals(2, headers.size());
        Assert.assertEquals("1", headers.get(0).getAttemptId());
        Assert.assertEquals("2", headers.get(1).getAttemptId());
    }

    private List<EventHeaderProtos.Header> checkSingleFileWithFileSystem(
            Collection<EventHeaderProtos.Header> inputHeaders) throws IOException {
        final java.nio.file.Path tmpDir = Files.createTempDirectory("hdfs-reader-test-");
        final List<EventHeaderProtos.Header> headers = new LinkedList<>();

        try {
            final Path baseDir = new Path(tmpDir.toString());
            final Path tmpPath = new Path(baseDir, "tmp");
            final FileSystem localFs = FileSystem.getLocal(new Configuration());
            final ProtoParquetWriter<Message> writer = new ProtoParquetWriter<>(tmpPath,
                    EventHeaderProtos.Header.class);
            long offset = 1;

            final ProtoParquetWriterWithOffset consumer = new ProtoParquetWriterWithOffset<>(writer, tmpPath, baseDir,
                    localFs, new FixedOffsetComputer(FINAL_FILE_NAME, 123), UTC_EPOCH);

            for (EventHeaderProtos.Header header : inputHeaders) {
                consumer.write(header, new TopicPartitionOffset(TOPIC, 1, offset++));
            }
            consumer.close();

            final RemoteIterator<LocatedFileStatus> filesIterator = localFs.listFiles(baseDir, false);
            final LocatedFileStatus fileStatus = filesIterator.next();
            Assert.assertEquals(FINAL_FILE_NAME, fileStatus.getPath().getName());
            Assert.assertFalse("There should be only one output file", filesIterator.hasNext());

            final ParquetReader<EventHeaderProtos.Header.Builder> reader;
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
