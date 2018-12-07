package com.criteo.hadoop.garmadon.hdfs.writer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class FileSystemUtilsTest {
    @Test
    public void ensureExistingDirectoryExists() throws IOException {
        FileSystem fsMock = mock(FileSystem.class);
        Path path = new Path("existing");

        when(fsMock.exists(any(Path.class))).thenReturn(true);

        FileSystemUtils.ensureDirectoriesExist(Collections.singleton(path), fsMock);
        verify(fsMock, times(1)).exists(path);
        verify(fsMock, times(0)).mkdirs(any(Path.class));
    }

    @Test
    public void ensureCreatedDirectoryExists() throws IOException {
        FileSystem fsMock = mock(FileSystem.class);
        Path path = new Path("existing");

        when(fsMock.exists(any(Path.class))).thenReturn(false);
        when(fsMock.mkdirs(any(Path.class))).thenReturn(true);

        FileSystemUtils.ensureDirectoriesExist(Collections.singleton(new Path("existing")), fsMock);
        verify(fsMock, times(1)).exists(path);
        verify(fsMock, times(1)).mkdirs(path);
    }

    @Test(expected = IOException.class)
    public void ensureNonExistingDirectoriesDontExist() throws IOException {
        FileSystem fsMock = mock(FileSystem.class);

        when(fsMock.exists(any(Path.class))).thenReturn(false);
        when(fsMock.mkdirs(any(Path.class))).thenReturn(false);

        FileSystemUtils.ensureDirectoriesExist(Arrays.asList(new Path("one"), new Path("two")), fsMock);
    }

    @Test(expected = IOException.class)
    public void ensureSomeExistingDirectoriesDontExist() throws IOException {
        FileSystem fsMock = mock(FileSystem.class);
        Path existingPath = new Path("existing");
        Path nonExistingPath = new Path("non existing");

        when(fsMock.exists(eq(existingPath))).thenReturn(true);
        when(fsMock.exists(eq(nonExistingPath))).thenReturn(false);
        when(fsMock.mkdirs(eq(nonExistingPath))).thenReturn(false);

        FileSystemUtils.ensureDirectoriesExist(Arrays.asList(existingPath, nonExistingPath), fsMock);
    }

    @Test(expected = IOException.class)
    public void ensureDirectoriesExistWhenExistsThrows() throws IOException {
        FileSystem fsMock = mock(FileSystem.class);

        when(fsMock.exists(any(Path.class))).thenThrow(new IOException("Ayo"));
        FileSystemUtils.ensureDirectoriesExist(Collections.singleton(new Path("randomPath")), fsMock);
    }

    @Test(expected = IOException.class)
    public void ensureDirectoriesExistWhenMkdirThrows() throws IOException {
        FileSystem fsMock = mock(FileSystem.class);

        when(fsMock.mkdirs(any(Path.class))).thenThrow(new IOException("Ayo"));
        FileSystemUtils.ensureDirectoriesExist(Collections.singleton(new Path("randomPath")), fsMock);
    }

    @Test
    public void ensureEmptyDirectoriesListExists() throws IOException {
        FileSystemUtils.ensureDirectoriesExist(Collections.emptyList(), mock(FileSystem.class));
    }
}
