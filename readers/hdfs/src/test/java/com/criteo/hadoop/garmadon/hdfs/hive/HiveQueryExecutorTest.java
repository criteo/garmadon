package com.criteo.hadoop.garmadon.hdfs.hive;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HiveQueryExecutorTest {
    private Path hdfsTemp;
    private SimpleHiveServer hiveServer;

    @Before
    public void setup() throws IOException {
        hdfsTemp = Files.createTempDirectory("hdfs");
        hiveServer = new SimpleHiveServer();
    }

    @After
    public void teardown() throws IOException {
        try {
            hiveServer.cleanup();
        }
        catch (IOException e) {
            // Nothing
        }

        FileUtils.deleteDirectory(hdfsTemp.toFile());
    }

    @Test
    public void shouldReconnectOnTTransportException() throws Exception {
        HiveQueryExecutor executor = spy(new HiveQueryExecutor(
            SimpleHiveServer.DRIVER_NAME, hiveServer.getJdbcString(), "garmadon"));

        executor.connect();
        executor.createDatabaseIfAbsent(hdfsTemp + "/garmadon_database");

        hiveServer.stop();

        // Ignore exception as hiveserver2 is down and we are testing that connect method is well called multiple times
        try {
            executor.createDatabaseIfAbsent(hdfsTemp + "/garmadon_database");
        } catch (SQLException ignored) {
        }

        verify(executor, times(6)).connect();
    }
}
