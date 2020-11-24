package com.criteo.hadoop.garmadon.hdfs.hive;

import org.apache.commons.io.FileUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class HiveClientTest {
    private static final String DATABASE_NAME = "garmadon";
    private static final PrimitiveType APP_ID_TYPE = new PrimitiveType(
        Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "app_id");
    private Path hdfsTemp;
    private SimpleHiveServer hiveServer2;

    @Before
    public void setup() throws IOException {
        hdfsTemp = Files.createTempDirectory("hdfs");
        hiveServer2 = new SimpleHiveServer();
    }

    @After
    public void close() throws IOException {
        try {
            hiveServer2.cleanup();
        }
        catch (IOException e) {
            // Nothing
        }

        FileUtils.deleteDirectory(hdfsTemp.toFile());
    }

    @Test
    public void connectAndCreateDatabaseWithoutIssue() throws SQLException {
        HiveQueryExecutor executor = new HiveQueryExecutor(SimpleHiveServer.DRIVER_NAME, hiveServer2.getJdbcString(), DATABASE_NAME);

        new HiveClient(executor, hdfsTemp + "/garmadon_database");

        executor.execute("DESCRIBE DATABASE " + DATABASE_NAME);
        try {
            executor.execute("DESCRIBE DATABASE not_created_database");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Database does not exist: not_created_database"));
        }
    }

    @Test(expected = SQLException.class)
    public void failConnectingDueToBadHost() throws SQLException {
        new HiveClient(new HiveQueryExecutor(SimpleHiveServer.DRIVER_NAME, "jdbc:hive2://bashost:123", DATABASE_NAME),
            hdfsTemp + "/garmadon_database");
    }

    @Test
    public void createTableWithoutIssue() throws SQLException {
        MessageType schema = new MessageType("fs", APP_ID_TYPE);

        String table = "fs";
        String location = "file:" + hdfsTemp + "/garmadon_database/fs";
        HiveQueryExecutor executor = new HiveQueryExecutor(SimpleHiveServer.DRIVER_NAME, hiveServer2.getJdbcString(), "garmadon");
        HiveClient hiveClient = new HiveClient(executor, hdfsTemp + "/garmadon_database");
        hiveClient.createTableIfNotExist(table, schema, location);

        HashMap<String, String> result = getResultHashTableDesc(executor, table);
        assertEquals(location, result.get("Location"));
        assertEquals("EXTERNAL_TABLE", result.get("Table Type").trim());
        assertEquals("string", result.get("day"));
        assertEquals("string", result.get("app_id"));

        hiveClient.createTableIfNotExist(table, schema, location);
    }

    @Test
    public void partitionCreationHasNoSideEffectWhenAlreadyDone() throws SQLException {
        final HiveQueryExecutor executor = mock(HiveQueryExecutor.class);
        final String table = "tableName";
        final String location = "location";
        final String partition = "partition";
        final MessageType schema = new MessageType("fs", APP_ID_TYPE);

        doNothing().when(executor).createTableIfNotExists(anyString(), anyString(), anyString());
        doNothing().when(executor).createPartition(anyString(), anyString(), anyString());

        HiveClient hiveClient = new HiveClient(executor, "/any");

        for (int times = 0; times < 2; ++times) {
            hiveClient.createPartitionIfNotExist(table, schema, partition, location);
            verify(executor, times(1)).createTableIfNotExists(anyString(), anyString(), anyString());
            verify(executor, times(1)).createPartition(anyString(), anyString(), anyString());
        }
    }

    @Test(expected = RuntimeException.class)
    public void tryCreateTableWithUnsupportedDataTypeShouldThrowRuntimeException() throws SQLException {
        PrimitiveType unsupported = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "unsupported");

        MessageType schema = new MessageType("fs", unsupported);

        String table = "fs";
        String location = "file:" + hdfsTemp + "/garmadon_database/fs";
        HiveClient hiveClient = new HiveClient(new HiveQueryExecutor(
            SimpleHiveServer.DRIVER_NAME, hiveServer2.getJdbcString(), "garmadon"),
            hdfsTemp + "/garmadon_database");
        hiveClient.createTableIfNotExist(table, schema, location);
    }

    @Test
    public void shouldProvideHiveTypeFromParquetType() throws Exception {
        HiveClient hiveClient = new HiveClient(new HiveQueryExecutor(
            SimpleHiveServer.DRIVER_NAME, hiveServer2.getJdbcString(), "garmadon"),
            hdfsTemp + "/garmadon_database");

        PrimitiveType string = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "name");
        assertEquals("string", hiveClient.inferHiveType(string));

        PrimitiveType array_string = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BINARY, "name");
        assertEquals("array<string>", hiveClient.inferHiveType(array_string));

        PrimitiveType int32 = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "name");
        assertEquals("int", hiveClient.inferHiveType(int32));

        PrimitiveType int64 = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "name");
        assertEquals("bigint", hiveClient.inferHiveType(int64));

        PrimitiveType floatz = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "name");
        assertEquals("float", hiveClient.inferHiveType(floatz));

        PrimitiveType doublez = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "name");
        assertEquals("double", hiveClient.inferHiveType(doublez));

        PrimitiveType booleanz = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "name");
        assertEquals("boolean", hiveClient.inferHiveType(booleanz));
    }


    @Test(expected = Exception.class)
    public void shouldThrowExceptionForUnknownParquetType() throws Exception {
        HiveClient hiveClient = new HiveClient(new HiveQueryExecutor(
            SimpleHiveServer.DRIVER_NAME, hiveServer2.getJdbcString(), "garmadon"),
            hdfsTemp + "/garmadon_database");

        PrimitiveType unsupported = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "unsupported");
        hiveClient.inferHiveType(unsupported);
    }

    private HashMap<String, String> getResultHashTableDesc(HiveQueryExecutor queryExecutor, String table) throws SQLException {
        HashMap<String, String> result = new HashMap<>();
        ResultSet rset = queryExecutor.getStmt().executeQuery("DESCRIBE FORMATTED " + DATABASE_NAME + "." + table);
        while (rset.next()) {
            result.put(rset.getString(1).split(":")[0], rset.getString(2));
        }
        rset.close();
        return result;
    }
}
