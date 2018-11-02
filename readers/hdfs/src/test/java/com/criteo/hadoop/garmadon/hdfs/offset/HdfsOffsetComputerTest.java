package com.criteo.hadoop.garmadon.hdfs.offset;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HdfsOffsetComputerTest {
    @Test
    public void fullyMatchingPattern() throws IOException {
        performSinglePartitionTest(Collections.singletonList("12"), "(.*)", 12);
    }

    @Test
    public void patternMatchingPart() throws IOException {
        performSinglePartitionTest(Collections.singletonList("something.12"), "^.*\\.(\\d+)$", 12);
    }

    @Test
    public void unorderedFiles() throws IOException {
        performSinglePartitionTest(Arrays.asList("1", "12", "10"), "(.*)", 12);
    }

    @Test
    public void noFile() throws IOException {
        performSinglePartitionTest(Collections.emptyList(), "(.*)", OffsetComputer.NO_OFFSET);
    }

    @Test
    public void nonNumericOffset() throws IOException {
        performSinglePartitionTest(Collections.singletonList("abc"), "(.*)", OffsetComputer.NO_OFFSET);
   }

    @Test
    public void matchingAndNotMaching() throws IOException {
        performSinglePartitionTest(Arrays.asList("abc", "12", "12e"), "(\\d+)", 12);
    }

    @Test
    public void nonMatchingPattern() throws IOException {
        performSinglePartitionTest(Collections.singletonList("12"), "(13)", OffsetComputer.NO_OFFSET);
    }

    @Test
    public void noGroupPattern() throws IOException {
        performSinglePartitionTest(Collections.singletonList("12"), "12", OffsetComputer.NO_OFFSET);
    }

    @Test
    public void matchingPatternAmongMultiplePartitions() throws IOException {
        final HdfsOffsetComputer offsetComputer = new HdfsOffsetComputer(buildFileSystem(
                Arrays.asList("456.12", "123.12", "456.24")),
                new Path("Fake path"),
                partitionId -> Pattern.compile(String.format("^%d\\.(\\d+)$", partitionId)));

        Assert.assertEquals(12, offsetComputer.compute(123));
    }

    @Test
    public void noMatchForPartition() throws IOException {
        final HdfsOffsetComputer offsetComputer = new HdfsOffsetComputer(buildFileSystem(
                Arrays.asList("456.12", "123.abc", "456.24")),
                new Path("Fake path"),
                partitionId -> Pattern.compile(String.format("^%d\\.(\\d+)$", partitionId)));

        Assert.assertEquals(OffsetComputer.NO_OFFSET, offsetComputer.compute(123));
    }

    private void performSinglePartitionTest(List<String> fileNames, String pattern, long expectedOffset) throws IOException {
        final HdfsOffsetComputer offsetComputer = new HdfsOffsetComputer(buildFileSystem(fileNames),
                new Path("Fake path"), partitionId -> Pattern.compile(pattern));

        Assert.assertEquals(expectedOffset, offsetComputer.compute(123));
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
