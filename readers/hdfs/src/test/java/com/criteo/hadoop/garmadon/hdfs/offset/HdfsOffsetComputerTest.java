package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.reader.Offset;
import com.criteo.hadoop.garmadon.reader.TopicPartitionOffset;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.*;

import static com.criteo.hadoop.garmadon.hdfs.TestUtils.localDateTimeFromDate;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HdfsOffsetComputerTest {
    @Test
    public void fullyMatchingFileName() throws IOException {
        performSinglePartitionTest(Collections.singletonList("123.12"), 123, 12);
    }

    @Test
    public void nonMatchingPartition() throws IOException {
        performSinglePartitionTest(Collections.singletonList("345.12"),  123, OffsetComputer.NO_OFFSET);
    }

    @Test
    public void noPartition() throws IOException {
        performSinglePartitionTest(Collections.singletonList("12"), 123, OffsetComputer.NO_OFFSET);
    }

    @Test
    public void unorderedFiles() throws IOException {
        performSinglePartitionTest(Arrays.asList("123.1", "123.12", "123.10"), 123, 12);
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
        performSinglePartitionTest(Arrays.asList("abc", "123.12", "12e"), 123, 12);
    }

    @Test
    public void matchingPatternAmongMultiplePartitions() throws IOException {
        final HdfsOffsetComputer offsetComputer = new HdfsOffsetComputer(buildFileSystem(
                Arrays.asList("456.12", "123.12", "456.24")),
                new Path("Fake path"));

        Assert.assertEquals(12L, offsetComputer.computeOffsets(Collections.singleton(123)).get(123).longValue());
    }

    @Test
    public void noMatchForPartition() throws IOException {
        final HdfsOffsetComputer offsetComputer = new HdfsOffsetComputer(buildFileSystem(
                Arrays.asList("456.12", "123.abc", "456.24")),
                new Path("Fake path"));

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

            final HdfsOffsetComputer hdfsOffsetComputer = new HdfsOffsetComputer(localFs, basePath);

            final LocalDateTime day1 = localDateTimeFromDate("1987-08-13 00:00:00");
            final LocalDateTime day2 = localDateTimeFromDate("1987-08-14 00:00:00");

            /*
                /tmp/hdfs-reader-test-1234
                └── embedded
                    ├── 1987-08-13
                    │   ├── 1.2
                    │   ├── 1.3
                    │   └── 2.12
                    └── 1987-08-14
                        └── 1.1
             */
            localFs.mkdirs(rootPath);
            localFs.mkdirs(basePath);
            localFs.create(new Path(basePath, hdfsOffsetComputer.computePath(day1, buildOffset(1, 2))));
            localFs.create(new Path(basePath, hdfsOffsetComputer.computePath(day1, buildOffset(1, 3))));
            localFs.create(new Path(basePath, hdfsOffsetComputer.computePath(day1, buildOffset(2, 12))));
            localFs.create(new Path(basePath, hdfsOffsetComputer.computePath(day2, buildOffset(1, 1))));

            Map<Integer, Long> offsets = hdfsOffsetComputer.computeOffsets(Arrays.asList(1, 2, 3));

            Assert.assertEquals(3, offsets.get(1).longValue());
            Assert.assertEquals(12, offsets.get(2).longValue());
            Assert.assertEquals(-1, offsets.get(3).longValue());
        }
        finally {
            FileUtils.deleteDirectory(tmpDir.toFile());
        }
    }

    private void performSinglePartitionTest(List<String> fileNames, int partitionId, long expectedOffset)
            throws IOException {
        final HdfsOffsetComputer offsetComputer = new HdfsOffsetComputer(buildFileSystem(fileNames),
                new Path("Fake path"));

        Assert.assertEquals(expectedOffset,
                offsetComputer.computeOffsets(Collections.singleton(partitionId)).get(partitionId).longValue());
    }

    private Offset buildOffset(int partition, long offset) {
        return new TopicPartitionOffset("Dummy topic", partition, offset);
    }

    private FileSystem buildFileSystem(List<String> fileNames) throws IOException {
        final FileSystem fs = mock(FileSystem.class);
        final Iterator<String> fileNamesIterator = fileNames.iterator();

        final RemoteIterator<LocatedFileStatus> files = new RemoteIterator<LocatedFileStatus>() {
            @Override
            public boolean hasNext() {
                return fileNamesIterator.hasNext();
            }

            @Override
            public LocatedFileStatus next() {
                final String next = fileNamesIterator.next();
                final LocatedFileStatus fileStatus = mock(LocatedFileStatus.class);

                when(fileStatus.getPath()).thenReturn(new Path(next));

                return fileStatus;
            }
        };

        when(fs.listFiles(any(Path.class), anyBoolean())).thenReturn(files);

        return fs;
    }
}
