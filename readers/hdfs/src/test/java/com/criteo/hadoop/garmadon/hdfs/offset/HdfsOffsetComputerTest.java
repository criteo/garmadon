package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.hdfs.EventsWithHeader;
import com.criteo.hadoop.garmadon.protobuf.ProtoConcatenator;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.criteo.hadoop.garmadon.reader.TopicPartitionOffset;
import com.google.protobuf.Message;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class HdfsOffsetComputerTest {
    private HdfsOffsetComputer offsetComputer;

    @Before
    public void setup() throws IOException {
        offsetComputer = new HdfsOffsetComputer(buildFileSystem(
            Arrays.asList("456.12", "123.abc", "456.24")),
            new Path("Fake path"), 2);
    }

    @Test
    public void fullyMatchingFileName() throws IOException {
        performSinglePartitionTest(Collections.singletonList("123.index=1.12"), 123, 12);
    }

    @Test
    public void nonMatchingPartition() throws IOException {
        performSinglePartitionTest(Collections.singletonList("345.12"), 123, OffsetComputer.NO_OFFSET);
    }

    @Test
    public void noPartition() throws IOException {
        performSinglePartitionTest(Collections.singletonList("12"), 123, OffsetComputer.NO_OFFSET);
    }

    @Test
    public void unorderedFiles() throws IOException {
        performSinglePartitionTest(Arrays.asList("123.index=1", "123.index=12", "123.index=10"), 123, 12);
    }

    @Test
    public void noFile() throws IOException {
        performSinglePartitionTest(Collections.emptyList(), 123, OffsetComputer.NO_OFFSET);
    }

    @Test
    public void nonNumericOffset() throws IOException {
        performSinglePartitionTest(Collections.singletonList("abc"), 123, OffsetComputer.NO_OFFSET);
    }

    @Test
    public void matchingAndNotMaching() throws IOException {
        performSinglePartitionTest(Arrays.asList("abc", "123.index=12", "12e"), 123, 12);
    }

    @Test
    public void getIndexReturnFileIndex() {
        Assert.assertEquals(1, offsetComputer.getIndex("123.index=1.12"));
    }

    @Test
    public void getIndexReturn0IfNoIndex() {
        Assert.assertEquals(0, offsetComputer.getIndex("123.12"));
    }

    @Test
    public void migrationToClusterInfo() throws IOException {
        performSinglePartitionTest(Arrays.asList("123.index=12", "123.cluster=pa4.index=13"), 123, 13, "pa4");
        performSinglePartitionTest(Arrays.asList("123.index=12", "123.cluster=pa4.index=13"), 123, 12);

        performSinglePartitionTest(Arrays.asList("42.index=23", "42.cluster=pa4.index=22"), 42, 22, "pa4");
        performSinglePartitionTest(Arrays.asList("42.index=23", "42.cluster=pa4.index=22"), 42, 23);
    }

    @Test
    public void matchingPatternAmongMultiplePartitions() throws IOException {
        final HdfsOffsetComputer offsetComputer = spy(new HdfsOffsetComputer(buildFileSystem(
            Arrays.asList("456.index=12", "123.index=12", "456.index=12")),
            new Path("Fake path"), 2));

        doReturn(12L).when(offsetComputer).getMaxOffset(any());

        Assert.assertEquals(12L, offsetComputer.computeOffsets(Collections.singleton(123)).get(123).longValue());
    }

    @Test
    public void noMatchForPartition() throws IOException {
        Assert.assertEquals(OffsetComputer.NO_OFFSET, offsetComputer.computeOffsets(Collections.singleton(123)).get(123).longValue());
    }

    @Test
    public void actualFileSystem() throws IOException {
        final java.nio.file.Path tmpDir = Files.createTempDirectory("hdfs-reader-test-");

        try {
            final Path rootPath = new Path(tmpDir.toString());
            // Make sure we can read from subdirectories
            final Path basePath = new Path(rootPath, "embedded");
            final FileSystem localFs = FileSystem.getLocal(new Configuration());

            final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, basePath, 2);

            final LocalDateTime today = LocalDateTime.now();
            final LocalDateTime yesterday = today.minusDays(1);
            final LocalDateTime twoDaysAgo = today.minusDays(2);

            /*
                /tmp/hdfs-reader-test-1234
                └── embedded
                    ├── <today>
                    │   ├── 1.index=0
                    │   └── 2.index=0
                    └── <yesterday>
                    │   ├── 1.index=0
                    │   └── 1.index=1
                    └── <2 days ago> # Should be ignored
                        └── 1.index=0
             */
            localFs.mkdirs(rootPath);
            localFs.mkdirs(basePath);

            writeParquetFile(new Path(basePath, hdfsOffsetComputer.computePath(today, 0L, 1)), 1);
            writeParquetFile(new Path(basePath, hdfsOffsetComputer.computePath(today, 0L, 2)), 12);
            writeParquetFile(new Path(basePath, hdfsOffsetComputer.computePath(yesterday, 0L, 1)), 2);
            writeParquetFile(new Path(basePath, hdfsOffsetComputer.computePath(yesterday, 1L, 1)), 3);
            writeParquetFile(new Path(basePath, hdfsOffsetComputer.computePath(twoDaysAgo, 0L, 1)), 42);

            Map<Integer, Long> offsets = hdfsOffsetComputer.computeOffsets(Arrays.asList(1, 2, 3));

            Assert.assertEquals(3, offsets.get(1).longValue());
            Assert.assertEquals(12, offsets.get(2).longValue());
            Assert.assertEquals(-1, offsets.get(3).longValue());
        } finally {
            FileUtils.deleteDirectory(tmpDir.toFile());
        }
    }

    private void performSinglePartitionTest(List<String> fileNames, int partitionId, long expectedOffset) throws IOException {
        performSinglePartitionTest(fileNames, partitionId, expectedOffset, null);
    }

    private void performSinglePartitionTest(List<String> fileNames, int partitionId, long expectedOffset, String kafkaCluster)
        throws IOException {
        final HdfsOffsetComputer offsetComputer = spy(new HdfsOffsetComputer(buildFileSystem(fileNames),
            new Path("Fake path"), kafkaCluster, 2));

        doReturn(expectedOffset).when(offsetComputer).getMaxOffset(any());

        Assert.assertEquals(expectedOffset,
            offsetComputer.computeOffsets(Collections.singleton(partitionId)).get(partitionId).longValue());
    }

    private Offset buildOffset(int partition, long offset) {
        return new TopicPartitionOffset("Dummy topic", partition, offset);
    }

    private FileSystem buildFileSystem(List<String> fileNames) throws IOException {
        final FileSystem fs = mock(FileSystem.class);
        final FileStatus[] statuses = fileNames.stream().map(name ->
            new FileStatus(0, false, 0, 0, 0, new Path(name))).toArray(FileStatus[]::new);

        when(fs.globStatus(any(Path.class))).thenReturn(statuses);

        return fs;
    }

    private void writeParquetFile(Path fileName, long offset) throws IOException {
        ProtoParquetWriter<Message> writer = new ProtoParquetWriter<>(fileName, EventsWithHeader.FsEvent.class, CompressionCodecName.SNAPPY,
            1 * 1_024 * 1_024, 1_024 * 1_024);

        EventHeaderProtos.Header emptyHeader = EventHeaderProtos.Header.newBuilder().build();

        LongStream.range(0, offset).forEach(n -> {
            Message msg = ProtoConcatenator
                .concatToProtobuf(System.currentTimeMillis(), offset, Arrays.asList(emptyHeader, DataAccessEventProtos.FsEvent.newBuilder().build()))
                .build();
            try {
                writer.write(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        writer.close();
    }
}
