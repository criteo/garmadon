package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
import com.criteo.hadoop.garmadon.hdfs.FixedOffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.monitoring.PrometheusMetrics;
import com.criteo.hadoop.garmadon.hdfs.offset.HdfsOffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.TopicPartitionOffset;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class ProtoParquetWriterWithOffsetTest {
    private static final String FINAL_FILE_NAME = "finalFile";
    private static final LocalDateTime UTC_EPOCH = LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC"));
    private static final String TOPIC = "topic";
    private static final LocalDateTime TODAY = LocalDateTime.now();

    private FileSystem localFs;
    private Path rootPath;
    private Path finalPath;
    private Path tmpPath;

    @Before
    public void setup() throws IOException {
        final java.nio.file.Path tmpDir = Files.createTempDirectory("hdfs-reader-test-");
        rootPath = new Path(tmpDir.toString());
        finalPath = new Path(rootPath, "final");
        tmpPath = new Path(rootPath, "tmp");
        localFs = FileSystem.getLocal(new Configuration());
        localFs.mkdirs(rootPath);
        localFs.mkdirs(finalPath);
        localFs.mkdirs(tmpPath);

        PrometheusMetrics.clearCollectors();
    }

    @After
    public void tearDown() throws IOException {
        localFs.delete(rootPath, true);
    }

    @Test(expected = IOException.class)
    public void closeWithNoEvent() throws IOException {
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final BiConsumer<String, String> protoMetadataWriter = mock(BiConsumer.class);

        final ProtoParquetWriterWithOffset parquetWriter = new ProtoParquetWriterWithOffset<>(writerMock,
            createTmpFile(), finalPath, localFs, new HdfsOffsetComputer(localFs, finalPath, 2), LocalDateTime.MIN, "ignored",
            protoMetadataWriter, 0);

        parquetWriter.close();
    }

    @Test
    public void closeRenameFails() throws IOException {
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final OffsetComputer fileNamer = mock(OffsetComputer.class);
        final BiConsumer<String, String> protoMetadataWriter = mock(BiConsumer.class);
        final FileSystem localFsSpy = spy(localFs);

        when(fileNamer.computeTopicGlob(any(LocalDateTime.class), anyInt())).thenReturn("ignored");
        when(fileNamer.computePath(any(LocalDateTime.class), any(Long.class), anyInt())).thenReturn("ignored");
        doReturn(false).when(localFsSpy).rename(any(Path.class), any(Path.class));

        final ProtoParquetWriterWithOffset parquetWriter = new ProtoParquetWriterWithOffset<>(writerMock,
            createTmpFile(), finalPath, localFsSpy, new HdfsOffsetComputer(localFsSpy, finalPath, 2), LocalDateTime.MIN, "ignored",
            protoMetadataWriter, 0);
        boolean thrown = false;

        // We need to write one event, otherwise we will fail with a "no message" error
        parquetWriter.write(1234567890L, mock(MessageOrBuilder.class), new TopicPartitionOffset(TOPIC, 1, 2));

        try {
            parquetWriter.close();
        } catch (IOException e) {
            thrown = true;
        }
        // Writer is closed, but rename failed
        verify(writerMock, times(1)).close();

        reset(writerMock);
        assertTrue(thrown);
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
    public void finalFileTooBigToBeMerged() throws IOException {
        localFs.getConf().set("fs.local.block.size", "1");

        final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, rootPath, 2);

        //we already create and populate tmp file as we will mock the underlying writer
        final Path tmpFile = new Path(tmpPath, "tmp_file");
        final Path existingFinalFile = new Path(finalPath, hdfsOffsetComputer.computePath(TODAY, 0L, 1));
        createParquetFile(
            tmpFile,
            DataAccessEventProtos.FsEvent.class,
            () -> DataAccessEventProtos.FsEvent.newBuilder().build(),
            654321
        );
        createParquetFile(
            existingFinalFile,
            DataAccessEventProtos.FsEvent.class,
            () -> DataAccessEventProtos.FsEvent.newBuilder().build(),
            123456
        );

        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final BiConsumer<String, String> protoMetadataWriter = mock(BiConsumer.class);

        ProtoParquetWriterWithOffset parquetWriter = new ProtoParquetWriterWithOffset<>(writerMock,
            tmpFile, finalPath, localFs, hdfsOffsetComputer, TODAY, "ignored",
            protoMetadataWriter, 1);
        //simul write action
        parquetWriter.write(654321, mock(MessageOrBuilder.class), new TopicPartitionOffset(TOPIC, 1, 0));
        parquetWriter.close();


        final Path nextFile = new Path(finalPath, hdfsOffsetComputer.computePath(TODAY, 1L, 1));

        Set<LocatedFileStatus> finalFiles = listFiles(localFs, finalPath);
        Set<LocatedFileStatus> tmpFiles = listFiles(localFs, tmpPath);

        assertEquals(2, finalFiles.size());
        assertTrue(containsFile(finalFiles, existingFinalFile));
        assertTrue(containsFile(finalFiles, nextFile));
        assertEquals(0, tmpFiles.size());

        checkFileLatestCommittedTimestamp(existingFinalFile, 123456);
        checkFileLatestCommittedTimestamp(nextFile, 654321);
    }

    @Test
    public void finalFileAndTempFilesMergedWhenFinalSizeIsNotBigEnough() throws IOException {
        localFs.getConf().set("fs.local.block.size", String.valueOf(Long.MAX_VALUE));

        final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, rootPath, 2);

        //we already create and populate tmp file as we will mock the underlying writer
        final Path tmpFile = new Path(tmpPath, "tmp_file");
        final Path existingFinalFile = new Path(finalPath, hdfsOffsetComputer.computePath(TODAY, 0L, 1));
        createParquetFile(
            tmpFile,
            DataAccessEventProtos.FsEvent.class,
            () -> DataAccessEventProtos.FsEvent.newBuilder().build(),
            987654321
        );
        createParquetFile(
            existingFinalFile,
            DataAccessEventProtos.FsEvent.class,
            () -> DataAccessEventProtos.FsEvent.newBuilder().build(),
            123456789
        );

        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final BiConsumer<String, String> protoMetadataWriter = mock(BiConsumer.class);

        ProtoParquetWriterWithOffset parquetWriter = new ProtoParquetWriterWithOffset<>(writerMock,
            tmpFile, finalPath, localFs, hdfsOffsetComputer, TODAY, "ignored",
            protoMetadataWriter, 1);
        //simul write action
        parquetWriter.write(999999999L, mock(MessageOrBuilder.class), new TopicPartitionOffset(TOPIC, 1, 0));
        parquetWriter.close();


        Set<LocatedFileStatus> finalFiles = listFiles(localFs, finalPath);
        Set<LocatedFileStatus> tmpFiles = listFiles(localFs, tmpPath);

        assertEquals(1, finalFiles.size());
        assertTrue(containsFile(finalFiles, existingFinalFile));
        assertEquals(0, tmpFiles.size());

        //Check that it can still be read after merge
        ParquetReader<DataAccessEventProtos.FsEvent.Builder> reader = ProtoParquetReader
            .<DataAccessEventProtos.FsEvent.Builder>builder(existingFinalFile).build();
        int count = 0;
        while (reader.read() != null) {
            count++;
        }
        assertEquals(2, count);

        //timestamp should be the one of the latest tmp file merged
        checkFileLatestCommittedTimestamp(existingFinalFile, 999999999);
    }

    @Test
    public void finalFileAndTempFilesNotMergedDueToDifferentSchema() throws IOException {
        localFs.getConf().set("fs.local.block.size", "1");

        final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, rootPath, 2);

        //we already create and populate tmp file as we will mock the underlying writer
        final Path tmpFile = new Path(tmpPath, "tmp_file");
        final Path existingFinalFile = new Path(finalPath, hdfsOffsetComputer.computePath(TODAY, 0L, 1));
        createParquetFile(
            tmpFile,
            DataAccessEventProtos.FsEvent.class,
            () -> DataAccessEventProtos.FsEvent.newBuilder().build(),
            987654321
        );
        createParquetFile(
            existingFinalFile,
            ResourceManagerEventProtos.ContainerEvent.class,
            () -> ResourceManagerEventProtos.ContainerEvent.newBuilder().build(),
            123456789
        );

        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final BiConsumer<String, String> protoMetadataWriter = mock(BiConsumer.class);

        ProtoParquetWriterWithOffset parquetWriter = new ProtoParquetWriterWithOffset<>(writerMock,
            tmpFile, finalPath, localFs, hdfsOffsetComputer, TODAY, "ignored",
            protoMetadataWriter, 1);
        //simul write action
        parquetWriter.write(987654321, mock(MessageOrBuilder.class), new TopicPartitionOffset(TOPIC, 1, 0));

        parquetWriter.close();


        final Path nextFile = new Path(finalPath, hdfsOffsetComputer.computePath(TODAY, 1L, 1));

        Set<LocatedFileStatus> finalFiles = listFiles(localFs, finalPath);
        Set<LocatedFileStatus> tmpFiles = listFiles(localFs, tmpPath);

        assertEquals(2, finalFiles.size());
        assertTrue(containsFile(finalFiles, existingFinalFile));
        assertTrue(containsFile(finalFiles, nextFile));
        assertEquals(0, tmpFiles.size());

        checkFileLatestCommittedTimestamp(existingFinalFile, 123456789);
        checkFileLatestCommittedTimestamp(nextFile, 987654321);
    }

    @Test
    public void initializedWithExistingIndexFiles() throws IOException {
        final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, finalPath, 2);
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final Path path = new Path(finalPath, hdfsOffsetComputer.computePath(TODAY, 0, 0));
        createParquetFile(
            path,
            DataAccessEventProtos.FsEvent.class,
            () -> DataAccessEventProtos.FsEvent.newBuilder().build(),
            1234567890
        );

        final BiConsumer<String, String> protoMetadataWriter = mock(BiConsumer.class);

        final ProtoParquetWriterWithOffset consumer = new ProtoParquetWriterWithOffset<>(writerMock, tmpPath,
            finalPath, localFs, hdfsOffsetComputer, TODAY, "eventName",
            protoMetadataWriter, 0);

        assertThat(
            PrometheusMetrics.latestCommittedTimestampGauge("eventName", 0).get(),
            is((double) 1234567890)
        );
    }

    @Test
    public void initializedWithoutExistingIndexFiles() {
        final LocalDateTime today = LocalDateTime.now();
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final BiConsumer<String, String> protoMetadataWriter = mock(BiConsumer.class);

        final ProtoParquetWriterWithOffset consumer = new ProtoParquetWriterWithOffset<>(writerMock, tmpPath,
            finalPath, localFs, new HdfsOffsetComputer(localFs, finalPath, 2), today, "eventName",
            protoMetadataWriter, 0);

        assertEquals(
            (double) System.currentTimeMillis(),
            PrometheusMetrics.latestCommittedTimestampGauge("eventName", 0).get(),
            1000
        );
    }

    @Test
    public void initializedWithExistingIndexFilesHavingNoMetadataForLatestCommittedTimestamp() throws IOException {
        final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, finalPath, 2);
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final Path path = new Path(finalPath, hdfsOffsetComputer.computePath(TODAY, 0, 0));
        createParquetFile(
            path,
            DataAccessEventProtos.FsEvent.class,
            () -> DataAccessEventProtos.FsEvent.newBuilder().build(),
            Optional.empty()
        );

        final BiConsumer<String, String> protoMetadataWriter = mock(BiConsumer.class);

        final ProtoParquetWriterWithOffset consumer = new ProtoParquetWriterWithOffset<>(writerMock, tmpPath,
            finalPath, localFs, hdfsOffsetComputer, TODAY, "eventName",
            protoMetadataWriter, 0);

        assertEquals(
            (double) System.currentTimeMillis(),
            PrometheusMetrics.latestCommittedTimestampGauge("eventName", 0).get(),
            1000
        );
    }

    @Test
    public void initializedWithIOExceptionWhenFetchingExistingIndexFiles() throws IOException {
        final LocalDateTime today = LocalDateTime.now();
        final ProtoParquetWriter<Message> writerMock = mock(ProtoParquetWriter.class);
        final BiConsumer<String, String> protoMetadataWriter = mock(BiConsumer.class);
        final FileSystem localFsSpy = spy(localFs);

        final ProtoParquetWriterWithOffset consumer = new ProtoParquetWriterWithOffset<>(writerMock, tmpPath,
            finalPath, localFsSpy, new HdfsOffsetComputer(localFsSpy, finalPath, 2), today, "eventName",
            protoMetadataWriter, 0);

        doThrow(new IOException()).when(localFsSpy).globStatus(any(Path.class));

        assertEquals(
            (double) System.currentTimeMillis(),
            PrometheusMetrics.latestCommittedTimestampGauge("eventName", 0).get(),
            1000
        );
    }

    private <M extends Message> void createParquetFile(Path p, Class<M> clazz, Supplier<M> msgBuilder, long latestCommittedTimestamp) throws IOException {
        createParquetFile(p, clazz, msgBuilder, Optional.of(latestCommittedTimestamp));
    }

    private <M extends Message> void createParquetFile(Path p, Class<M> clazz, Supplier<M> msgBuilder, Optional<Long> latestCommittedTimestamp) throws IOException {
        ExtraMetadataWriteSupport extraMetadataWriteSupport = new ExtraMetadataWriteSupport<>(new ProtoWriteSupport<>(clazz));
        ParquetWriter<Message> writer = new ParquetWriter<>(p, extraMetadataWriteSupport, CompressionCodecName.SNAPPY,
            1_024 * 1_024, 1_024 * 1_024);

        writer.write(msgBuilder.get());
        latestCommittedTimestamp.ifPresent(
            timestamp -> extraMetadataWriteSupport.accept(ProtoParquetWriterWithOffset.LATEST_TIMESTAMP_META_KEY, String.valueOf(timestamp))
        );
        writer.close();
    }

    private void checkFileLatestCommittedTimestamp(Path p, long timestamp) throws IOException {
        ParquetFileReader reader = new ParquetFileReader(
            HadoopInputFile.fromPath(p, new Configuration()),
            ParquetReadOptions.builder().build()
        );
        String actualTimestamp = reader.getFooter().getFileMetaData().getKeyValueMetaData().get(ProtoParquetWriterWithOffset.LATEST_TIMESTAMP_META_KEY);
        assertThat(actualTimestamp, is(String.valueOf(timestamp)));
    }

    private List<EventHeaderProtos.Header> checkSingleFileWithFileSystem(
        Collection<EventHeaderProtos.Header> inputHeaders) throws IOException {
        final List<EventHeaderProtos.Header> headers = new LinkedList<>();

        Path newTmpFile = new Path(tmpPath, "file");
        final ProtoParquetWriter<Message> writer = new ProtoParquetWriter<>(newTmpFile,
            EventHeaderProtos.Header.class);
        long offset = 1;
        final BiConsumer<String, String> protoMetadataWriter = mock(BiConsumer.class);

        final ProtoParquetWriterWithOffset consumer = new ProtoParquetWriterWithOffset<>(writer, newTmpFile, finalPath,
            localFs, new FixedOffsetComputer(FINAL_FILE_NAME, 123), UTC_EPOCH, "ignored",
            protoMetadataWriter, 1);

        for (EventHeaderProtos.Header header : inputHeaders) {
            consumer.write(1234567890L, header, new TopicPartitionOffset(TOPIC, 1, offset++));
        }
        consumer.close();

        final RemoteIterator<LocatedFileStatus> filesIterator = localFs.listFiles(finalPath, false);
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

    private Path createTmpFile() throws IOException {
        Path p = new Path(tmpPath, UUID.randomUUID().toString());
        localFs.create(p).close();
        return p;
    }

    private Set<LocatedFileStatus> listFiles(FileSystem fs, Path p) throws IOException {
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(p, true);
        Set<LocatedFileStatus> s = new HashSet<>();
        while (it.hasNext()) {
            s.add(it.next());
        }
        return s;
    }

    private boolean containsFile(Set<LocatedFileStatus> files, Path p) {
        return files.stream().filter(lfs -> lfs.isFile() && pathWithoutScheme(lfs.getPath()).equals(pathWithoutScheme(p))).count() == 1;
    }

    private String pathWithoutScheme(Path p) {
        return p.toUri().getPath();
    }

}
