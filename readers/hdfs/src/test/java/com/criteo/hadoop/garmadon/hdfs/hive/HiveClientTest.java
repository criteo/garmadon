package com.criteo.hadoop.garmadon.hdfs.hive;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class HiveClientTest {
    private String driverName = "org.apache.hive.jdbc.HiveDriver";

    private Path hdfsTemp;
    private Path derbyDBPath;
    private String port;
    private HiveServer2 hiveServer2;
    private String database = "garmadon";

    @Before
    public void setup() throws IOException {
        hdfsTemp = Files.createTempDirectory("hdfs");
        derbyDBPath = Files.createTempDirectory("derbyDB");

        HiveConf hiveConf = new HiveConf();
        hiveConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(), "jdbc:derby:;databaseName=" +
            derbyDBPath.toString() + "/derbyDB" + ";create=true");

        ServerSocket s = new ServerSocket(0);
        port = String.valueOf(s.getLocalPort());
        hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, port);
        // Required to avoid NoSuchMethodError org.apache.hive.service.cli.operation.LogDivertAppender.setWriter
        hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED.varname, "false");

        hiveServer2 = new HiveServer2();
        hiveServer2.init(hiveConf);
        s.close();
        hiveServer2.start();
    }

    @After
    public void close() throws IOException {
        hiveServer2.stop();
        FileUtils.deleteDirectory(derbyDBPath.toFile());
        FileUtils.deleteDirectory(hdfsTemp.toFile());
    }

    @Test
    public void connectAndCreateDatabaseWithoutIssue() throws SQLException {
        HiveClient hiveClient = new HiveClient(driverName, "jdbc:hive2://localhost:" + port, database,
            hdfsTemp + "/garmadon_database");

        hiveClient.execute("DESCRIBE DATABASE " + database);
        try {
            hiveClient.execute("DESCRIBE DATABASE not_created_database");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Database does not exist: not_created_database"));
        }
    }

    @Test(expected = SQLException.class)
    public void failConnectingDueToBadHost() throws SQLException {
        new HiveClient(driverName, "jdbc:hive2://bashost:" + port, database, hdfsTemp + "/garmadon_database");
    }

    @Test
    public void createTableWithoutIssue() throws SQLException {
        PrimitiveType appId = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "app_id");

        MessageType schema = new MessageType("fs", appId);

        String table = "fs";
        String location = "file:" + hdfsTemp + "/garmadon_database/fs";
        HiveClient hiveClient = new HiveClient(driverName, "jdbc:hive2://localhost:" + port, "garmadon",
            hdfsTemp + "/garmadon_database");
        hiveClient.createTableIfNotExist(table, schema, location);

        HashMap<String, String> result = getResultHashTableDesc(hiveClient, table);
        assertEquals(location, result.get("Location"));
        assertEquals("EXTERNAL_TABLE", result.get("Table Type").trim());
        assertEquals("string", result.get("day"));
        assertEquals("string", result.get("app_id"));
    }

    @Test(expected = RuntimeException.class)
    public void tryCreateTableWithUnsupportedDataTypeShouldThrowRuntimeException() throws SQLException {
        PrimitiveType unsupported = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "unsupported");

        MessageType schema = new MessageType("fs", unsupported);

        String table = "fs";
        String location = "file:" + hdfsTemp + "/garmadon_database/fs";
        HiveClient hiveClient = new HiveClient(driverName, "jdbc:hive2://localhost:" + port, "garmadon",
            hdfsTemp + "/garmadon_database");
        hiveClient.createTableIfNotExist(table, schema, location);
    }

    @Test
    public void shouldProvideHiveTypeFromParquetType() throws Exception {
        HiveClient hiveClient = new HiveClient(driverName, "jdbc:hive2://localhost:" + port, "garmadon",
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
        HiveClient hiveClient = new HiveClient(driverName, "jdbc:hive2://localhost:" + port, "garmadon",
            hdfsTemp + "/garmadon_database");

        PrimitiveType unsupported = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "unsupported");
        hiveClient.inferHiveType(unsupported);
    }

    @Test
    public void shouldReconnectOnTTransportException() throws Exception {
        HiveClient hiveClient = spy(new HiveClient(driverName, "jdbc:hive2://localhost:" + port, "garmadon",
            hdfsTemp + "/garmadon_database"));

        hiveServer2.stop();

        // Ignore exception as hiveserver2 is down and we are testing that connect method is well called multiple times
        try {
            hiveClient.createDatabaseIfAbsent(hdfsTemp + "/garmadon_database");
        } catch (SQLException ignored) {
        }

        verify(hiveClient, times(5)).connect();
    }

    private HashMap<String, String> getResultHashTableDesc(HiveClient hiveClient, String table) throws SQLException {
        HashMap<String, String> result = new HashMap();
        ResultSet rset = hiveClient.getStmt().executeQuery("DESCRIBE FORMATTED " + database + "." + table);
        while (rset.next()) {
            result.put(rset.getString(1).split(":")[0], rset.getString(2));
        }
        rset.close();
        return result;
    }
}
