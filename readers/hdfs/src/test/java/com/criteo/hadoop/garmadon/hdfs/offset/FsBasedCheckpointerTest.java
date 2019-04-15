package com.criteo.hadoop.garmadon.hdfs.offset;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class FsBasedCheckpointerTest {
    @Test
    public void createNonExistingCheckpoints() throws IOException {
        FileSystem fsMock = mock(FileSystem.class);
        Instant firstDateTime = Instant.MIN;
        Instant secondDateTime = Instant.MAX;
        Path firstPath = new Path("testPath");
        Path secondPath = new Path("secondPath");
        FSDataOutputStream firstOsMock = mock(FSDataOutputStream.class);
        FSDataOutputStream secondOsMock = mock(FSDataOutputStream.class);
        FsBasedCheckpointer cptr = new FsBasedCheckpointer(fsMock, instant -> {
            if (instant == firstDateTime)
                return firstPath;
            if (instant == secondDateTime)
                return secondPath;

            throw new IllegalStateException("You should not be here");
        });

        when(fsMock.exists(firstPath)).thenReturn(false);
        when(fsMock.exists(secondPath)).thenReturn(false);
        when(fsMock.create(firstPath)).thenReturn(firstOsMock);
        when(fsMock.create(secondPath)).thenReturn(secondOsMock);

        Assert.assertTrue(cptr.tryCheckpoint(firstDateTime));
        verify(fsMock, times(1)).exists(firstPath);
        verify(fsMock, times(1)).create(firstPath);

        Assert.assertTrue(cptr.tryCheckpoint(secondDateTime));
        verify(fsMock, times(1)).exists(secondPath);
        verify(fsMock, times(1)).create(secondPath);

        verifyNoMoreInteractions(fsMock);

        verify(firstOsMock, times(1)).close();
        verify(secondOsMock, times(1)).close();
        verifyNoMoreInteractions(firstOsMock);
    }

    @Test
    public void recreateFailingCheckpoint() throws IOException {
        FileSystem fsMock = mock(FileSystem.class);
        FSDataOutputStream outputStreamMock = mock(FSDataOutputStream.class);
        Path returnedPath = new Path("testPath");
        FsBasedCheckpointer cptr = new FsBasedCheckpointer(fsMock, instant -> returnedPath);

        // Fail to check that checkpoint exists
        doThrow(new IOException("Ayo")).when(fsMock).exists(returnedPath);
        Assert.assertFalse(cptr.tryCheckpoint(Instant.MIN));

        // Fail to write checkpoint
        doReturn(false).when(fsMock).exists(returnedPath);
        doThrow(new IOException("Ayo")).when(fsMock).create(returnedPath);
        Assert.assertFalse(cptr.tryCheckpoint(Instant.MIN));

        // Should succeed now
        doReturn(outputStreamMock).when(fsMock).create(returnedPath);
        Assert.assertTrue(cptr.tryCheckpoint(Instant.MIN));

        // Succeeds once, fails once
        verify(fsMock, times(2)).create(returnedPath);

        verify(outputStreamMock, times(1)).close();
        verifyNoMoreInteractions(outputStreamMock);
    }

    @Test
    public void recreateExistingCachedCheckpoint() throws IOException {
        FileSystem fsMock = mock(FileSystem.class);
        Path returnedPath = new Path("testPath");
        FsBasedCheckpointer cptr = new FsBasedCheckpointer(fsMock, instant -> returnedPath);
        FSDataOutputStream outputStreamMock = mock(FSDataOutputStream.class);

        doReturn(outputStreamMock).when(fsMock).create(returnedPath);
        when(fsMock.exists(returnedPath)).thenReturn(true);
        when(fsMock.create(returnedPath)).thenReturn(outputStreamMock);

        Assert.assertFalse(cptr.tryCheckpoint(Instant.MIN));
        verify(fsMock, times(1)).exists(returnedPath);

        Assert.assertFalse(cptr.tryCheckpoint(Instant.MIN));
        verify(fsMock, never()).create(returnedPath);
        // Second time, the cache should be used
        verifyNoMoreInteractions(fsMock);

        verifyZeroInteractions(outputStreamMock);
    }

    @Test
    public void recreateExistingFsCheckpoint() throws IOException {
        FileSystem fsMock = mock(FileSystem.class);
        Path returnedPath = new Path("testPath");
        when(fsMock.exists(returnedPath)).thenReturn(true);

        FsBasedCheckpointer cptr = new FsBasedCheckpointer(fsMock, instant -> returnedPath);
        Assert.assertFalse(cptr.tryCheckpoint(Instant.MIN));

        // Don't use the same cache, to make sure FS is invoked for file existence
        FsBasedCheckpointer otherCptr = new FsBasedCheckpointer(fsMock, instant -> returnedPath);
        Assert.assertFalse(otherCptr.tryCheckpoint(Instant.MIN));

        verify(fsMock, times(2)).exists(returnedPath);
    }

    @Test
    public void actualFileSystem() throws IOException {
        final java.nio.file.Path tmpDir = Files.createTempDirectory("hdfs-reader-test-");

        try {
            final Path rootPath = new Path(tmpDir.toString());
            final FileSystem localFs = FileSystem.getLocal(new Configuration());

            final FsBasedCheckpointer cptr = new FsBasedCheckpointer(localFs, instant ->
                    new Path(rootPath, DateTimeFormatter.ofPattern("YYYY-MM-dd").format(instant.atZone(ZoneId.of("UTC")))));

            final Instant firstDay = Instant.parse("2019-04-02T12:25:00.00Z");
            final Instant secondDay = Instant.parse("2019-04-15T23:42:51.00Z");

            localFs.mkdirs(rootPath);
            Assert.assertTrue(cptr.tryCheckpoint(firstDay));
            Assert.assertFalse(cptr.tryCheckpoint(firstDay));
            Assert.assertTrue(cptr.tryCheckpoint(secondDay));

            List<FileStatus> fileStatuses = new ArrayList<>();
            Collections.addAll(fileStatuses, localFs.listStatus(rootPath));
            Assert.assertEquals(2, fileStatuses.size());
            Assert.assertTrue(fileStatuses.stream().anyMatch(f -> f.getPath().getName().equals("2019-04-02")));
            Assert.assertTrue(fileStatuses.stream().anyMatch(f -> f.getPath().getName().equals("2019-04-15")));
        }
        finally {
            FileUtils.deleteDirectory(tmpDir.toFile());
        }
    }
}
