package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.hdfs.EventsWithHeader;
import com.criteo.hadoop.garmadon.hdfs.FixedOffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.offset.HdfsOffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.protobuf.ProtoConcatenator;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.criteo.hadoop.garmadon.reader.TopicPartitionOffset;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class ProtoParquetWriterWithOffsetTest {
    private static final String FINAL_FILE_NAME = "finalFile";
    private static final LocalDateTime UTC_EPOCH = LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC"));
    private static final String TOPIC = "topic";
    private static final LocalDateTime TODAY = LocalDateTime.now();


    @Test
    public void closeWithSomeEvents() throws IOException {
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final Path tmpPath = new Path("tmp");
        final Path finalPath = new Path("final");
        final FileSystem fsMock = mock(FileSystem.class);
        final Message firstMessageMock = mock(Message.class);
        final Message secondMessageMock = mock(Message.class);
        final ProtoParquetWriterWithOffset consumer = new ProtoParquetWriterWithOffset<>(writerMock, tmpPath,
            finalPath, fsMock, new FixedOffsetComputer(FINAL_FILE_NAME, 123), LocalDateTime.MIN, "ignored");

        consumer.write(1234567890L, firstMessageMock, new TopicPartitionOffset(TOPIC, 1, 2));
        consumer.write(1234567891L, secondMessageMock, new TopicPartitionOffset(TOPIC, 1, 3));
        verify(writerMock, times(1)).write(firstMessageMock);
        verify(writerMock, times(1)).write(secondMessageMock);
        verifyNoMoreInteractions(writerMock);

        // Directory doesn't exist and creation succeeds
        when(fsMock.exists(any(Path.class))).thenReturn(false);
        when(fsMock.mkdirs(any(Path.class))).thenReturn(true);

        when(fsMock.rename(any(Path.class), any(Path.class))).thenReturn(true);
        when(fsMock.globStatus(any(Path.class))).thenReturn(new FileStatus[] {});

        consumer.close();

        verify(fsMock, times(1)).getDefaultBlockSize(eq(finalPath));
        verify(fsMock, times(1)).rename(tmpPath, new Path(finalPath, FINAL_FILE_NAME));
        verify(fsMock, times(1)).globStatus(new Path(finalPath, FINAL_FILE_NAME + "*"));
        verify(fsMock, times(1)).exists(eq(finalPath));
        verify(fsMock, times(1)).mkdirs(eq(finalPath));
        verifyNoMoreInteractions(fsMock);
    }

    @Test
    public void closeWithExistingIndexFile() throws IOException {
        final LocalDateTime today = LocalDateTime.now();
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final Path tmpPath = new Path("tmp");
        final Path finalPath = new Path("final");
        final FileSystem fsMock = mock(FileSystem.class);
        final FileStatus fileStatusMock = mock(FileStatus.class);
        final Path path = new Path(finalPath, today.format(DateTimeFormatter.ISO_DATE) + "/1.index=1");
        final Path pathRes = new Path(finalPath, today.format(DateTimeFormatter.ISO_DATE) + "/1.index=2");
        final Message firstMessageMock = mock(Message.class);
        final Message secondMessageMock = mock(Message.class);
        final ProtoParquetWriterWithOffset consumer = new ProtoParquetWriterWithOffset<>(writerMock, tmpPath,
            finalPath, fsMock, new HdfsOffsetComputer(fsMock, finalPath, 2), today, "ignored");

        consumer.write(1234567890L, firstMessageMock, new TopicPartitionOffset(TOPIC, 1, 2));
        consumer.write(1234567891L, secondMessageMock, new TopicPartitionOffset(TOPIC, 1, 3));

        // Directory doesn't exist and creation succeeds
        when(fsMock.exists(any(Path.class))).thenReturn(false);
        when(fsMock.mkdirs(any(Path.class))).thenReturn(true);

        when(fsMock.rename(any(Path.class), any(Path.class))).thenReturn(true);
        when(fsMock.globStatus(any(Path.class))).thenReturn(new FileStatus[] {fileStatusMock});

        when(fsMock.getFileStatus(any(Path.class))).thenReturn(fileStatusMock);

        when(fileStatusMock.getLen()).thenReturn(Long.MAX_VALUE);

        when(fileStatusMock.getPath()).thenReturn(path);

        consumer.close();

        verify(fsMock, times(1)).rename(tmpPath, pathRes);
    }

    @Test(expected = IOException.class)
    public void closeWithNoEvent() throws IOException {
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final FileSystem fsMock = mock(FileSystem.class);

        final ProtoParquetWriterWithOffset parquetWriter = new ProtoParquetWriterWithOffset<>(writerMock,
            new Path("tmp"), new Path("final"), fsMock, null, LocalDateTime.MIN, "ignored");

        parquetWriter.close();
    }

    @Test
    public void closeRenameFails() throws IOException {
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final FileSystem fsMock = mock(FileSystem.class);
        final OffsetComputer fileNamer = mock(OffsetComputer.class);
        final ProtoParquetWriterWithOffset parquetWriter = new ProtoParquetWriterWithOffset<>(writerMock,
            new Path("tmp"), new Path("final"), fsMock, fileNamer, LocalDateTime.MIN, "ignored");
        boolean thrown = false;

        // We need to write one event, otherwise we will fail with a "no message" error
        parquetWriter.write(1234567890L, mock(MessageOrBuilder.class), new TopicPartitionOffset(TOPIC, 1, 2));

        when(fileNamer.computeTopicGlob(any(LocalDateTime.class), any(Offset.class))).thenReturn("ignored");
        when(fileNamer.computePath(any(LocalDateTime.class), any(Long.class), any(Offset.class))).thenReturn("ignored");
        when(fsMock.rename(any(Path.class), any(Path.class))).thenReturn(false);
        when(fsMock.globStatus(any(Path.class))).thenReturn(new FileStatus[] {});
        try {
            parquetWriter.close();
        } catch (IOException e) {
            thrown = true;
        }
        // Writer is closed, but rename failed
        verify(writerMock, times(1)).close();

        reset(writerMock);
        Assert.assertTrue(thrown);
        try {
            parquetWriter.close();
        } catch (IOException ignored) {
        }
        // Writer already closed, so no more interaction
        verifyZeroInteractions(writerMock);
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

    @Test
    public void noFinalFileMoveTempOne() throws IOException {
        final java.nio.file.Path tmpDir = Files.createTempDirectory("hdfs-reader-test-");
        final Path rootPath = new Path(tmpDir.toString());
        final Path basePath = new Path(rootPath, "embedded");
        final FileSystem localFs = FileSystem.getLocal(new Configuration());

        final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, basePath, 2);

        localFs.mkdirs(rootPath);
        localFs.mkdirs(basePath);

        ProtoParquetWriterWithOffset parquetWriter = writeParquetFile(
            localFs,
            basePath,
            new Path(basePath, "tmp/" + hdfsOffsetComputer.computePath(TODAY, 0L, new TopicPartitionOffset(TOPIC, 1, 2))),
            hdfsOffsetComputer);

        parquetWriter.close();
        verify(parquetWriter, times(1)).moveToFinalPath(any(Path.class), any(Path.class));
        verify(parquetWriter, times(0)).mergeToFinalPath(any(Path.class), any(Path.class));

    }

    @Test
    public void finalFileTooBigToBeMerged() throws IOException {
        final java.nio.file.Path tmpDir = Files.createTempDirectory("hdfs-reader-test-");
        final Path rootPath = new Path(tmpDir.toString());
        final Path basePath = new Path(rootPath, "embedded");
        final FileSystem localFs = spy(FileSystem.getLocal(new Configuration()));

        doReturn(1L).when(localFs).getDefaultBlockSize(any());

        final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, basePath, 2);

        localFs.mkdirs(rootPath);
        localFs.mkdirs(basePath);

        writeParquetFile(
            localFs,
            basePath,
            new Path(basePath, "tmp/" + hdfsOffsetComputer.computePath(TODAY, 0L, new TopicPartitionOffset(TOPIC, 1, 2))),
            hdfsOffsetComputer).close();

        ProtoParquetWriterWithOffset parquetWriter = writeParquetFile(
            localFs,
            basePath,
            new Path(basePath, "tmp/" + hdfsOffsetComputer.computePath(TODAY, 1L, new TopicPartitionOffset(TOPIC, 1, 2))),
            hdfsOffsetComputer);

        parquetWriter.close();
        verify(parquetWriter, times(1)).moveToFinalPath(any(Path.class), any(Path.class));
        verify(parquetWriter, times(0)).mergeToFinalPath(any(Path.class), any(Path.class));
    }

    @Test
    public void finalFileAndTempFilesMerged() throws IOException {
        final java.nio.file.Path tmpDir = Files.createTempDirectory("hdfs-reader-test-");
        final Path rootPath = new Path(tmpDir.toString());
        final Path basePath = new Path(rootPath, "embedded");
        final FileSystem localFs = spy(FileSystem.getLocal(new Configuration()));

        doReturn(Long.MAX_VALUE).when(localFs).getDefaultBlockSize(any());

        final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, basePath, 2);

        localFs.mkdirs(rootPath);
        localFs.mkdirs(basePath);

        Path tmpFile1 = new Path(basePath, "tmp/" + hdfsOffsetComputer.computePath(TODAY, 1L, new TopicPartitionOffset(TOPIC, 1, 2)));
        writeParquetFile(
            localFs,
            basePath,
            tmpFile1,
            hdfsOffsetComputer).close();

        Path tmpFile2 = new Path(basePath, "tmp/" + hdfsOffsetComputer.computePath(TODAY, 2L, new TopicPartitionOffset(TOPIC, 1, 2)));
        ProtoParquetWriterWithOffset parquetWriter = writeParquetFile(
            localFs,
            basePath,
            tmpFile2,
            hdfsOffsetComputer);

        parquetWriter.close();
        verify(parquetWriter, times(1)).mergeToFinalPath(eq(new Path("file:" + tmpFile1.toString().replace("embedded/tmp", "embedded/final"))),
            any(Path.class));
        verify(parquetWriter, times(1)).moveToFinalPath(eq(new Path(tmpFile2.toString() + ".merged")), any(Path.class));

    }

    @Test
    public void finalFileAndTempFilesNotMergedDueToDifferentSchema() throws IOException {
        final java.nio.file.Path tmpDir = Files.createTempDirectory("hdfs-reader-test-");
        final Path rootPath = new Path(tmpDir.toString());
        final Path basePath = new Path(rootPath, "embedded");
        final FileSystem localFs = spy(FileSystem.getLocal(new Configuration()));

        doReturn(Long.MAX_VALUE).when(localFs).getDefaultBlockSize(any());

        final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, basePath, 2);

        localFs.mkdirs(rootPath);
        localFs.mkdirs(basePath);

        writeParquetFile(
            localFs,
            basePath,
            new Path(basePath, "tmp/" + hdfsOffsetComputer.computePath(TODAY, 0L, new TopicPartitionOffset(TOPIC, 1, 2))),
            hdfsOffsetComputer).close();

        Path tmpFile = new Path(basePath, "tmp/" + hdfsOffsetComputer.computePath(TODAY, 1L, new TopicPartitionOffset(TOPIC, 1, 2)));
        ProtoParquetWriterWithOffset parquetWriter = writeParquetFile2(
            localFs,
            basePath,
            tmpFile,
            hdfsOffsetComputer);

        parquetWriter.close();
        verify(parquetWriter, times(1)).mergeToFinalPath(eq(new Path("file:" + tmpFile.toString().replace("embedded/tmp", "embedded/final"))),
            any(Path.class));
        verify(parquetWriter, times(1)).moveToFinalPath(eq(tmpFile), any(Path.class));

    }

    private ProtoParquetWriterWithOffset writeParquetFile(FileSystem localFs, Path basePath, Path fileName, HdfsOffsetComputer hdfsOffsetComputer) throws IOException {
        ProtoParquetWriter<Message> writer = new ProtoParquetWriter<>(fileName, EventsWithHeader.FsEvent.class, CompressionCodecName.SNAPPY,
            1_024 * 1_024, 1_024 * 1_024);

        final ProtoParquetWriterWithOffset parquetWriter = spy(new ProtoParquetWriterWithOffset<>(writer,
            fileName, new Path(basePath, "final"), localFs, hdfsOffsetComputer, TODAY, "ignored"));

        EventHeaderProtos.Header emptyHeader = EventHeaderProtos.Header.newBuilder().build();

        Message.Builder protoConcatenator = ProtoConcatenator
            .concatToProtobuf(System.currentTimeMillis(), 1L, Arrays.asList(emptyHeader, DataAccessEventProtos.FsEvent.newBuilder().build()));

        Message msg = protoConcatenator.build();
        parquetWriter.write(1234567890L, msg, new TopicPartitionOffset(TOPIC, 1, 2));

        return parquetWriter;
    }

    private ProtoParquetWriterWithOffset writeParquetFile2(FileSystem localFs, Path basePath, Path fileName, HdfsOffsetComputer hdfsOffsetComputer) throws IOException {
        ProtoParquetWriter<Message> writer = new ProtoParquetWriter<>(fileName, EventsWithHeader.ContainerResourceEvent.class, CompressionCodecName.SNAPPY,
            1_024 * 1_024, 1_024 * 1_024);

        final ProtoParquetWriterWithOffset parquetWriter = spy(new ProtoParquetWriterWithOffset<>(writer,
            fileName, new Path(basePath, "final"), localFs, hdfsOffsetComputer, TODAY, "ignored"));

        EventHeaderProtos.Header emptyHeader = EventHeaderProtos.Header.newBuilder().build();

        Message.Builder protoConcatenator = ProtoConcatenator
            .concatToProtobuf(System.currentTimeMillis(), 1L, Arrays.asList(emptyHeader, ContainerEventProtos.ContainerResourceEvent.newBuilder().build()));

        Message msg = protoConcatenator.build();
        parquetWriter.write(1234567890L, msg, new TopicPartitionOffset(TOPIC, 1, 2));

        return parquetWriter;
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
                localFs, new FixedOffsetComputer(FINAL_FILE_NAME, 123), UTC_EPOCH, "ignored");

            for (EventHeaderProtos.Header header : inputHeaders) {
                consumer.write(1234567890L, header, new TopicPartitionOffset(TOPIC, 1, offset++));
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
        } finally {
            FileUtils.deleteDirectory(tmpDir.toFile());
        }
    }
}
