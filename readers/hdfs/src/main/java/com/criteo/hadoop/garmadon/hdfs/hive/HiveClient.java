package com.criteo.hadoop.garmadon.hdfs.hive;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

public class HiveClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveClient.class);

    private final String database;
    private final String jdbcUrl;
    private Connection connection;
    private Statement stmt;

    public HiveClient(String driverName, String jdbcUrl, String database, String location) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        this.database = database;
        this.jdbcUrl = jdbcUrl;
        // TODO discover mesos slave to contact
        connect();
        createDatabaseIfAbsent(location);
    }

    protected void connect() throws SQLException {
        connection = DriverManager.getConnection(jdbcUrl);
        stmt = connection.createStatement();
    }

    protected void execute(String query) throws SQLException {
        LOGGER.debug("Execute hql: {}", query);
        int maxAttempts = 5;
        for (int retry = 1; retry <= maxAttempts; ++retry) {
            try {
                stmt.execute(query);
            } catch (SQLException sqlException) {
                if (ExceptionUtils.indexOfThrowable(sqlException, TTransportException.class) != -1) {
                    String exMsg = String.format("Retry connecting to hive (%d/%d)", retry, maxAttempts);
                    if (retry <= maxAttempts) {
                        LOGGER.warn(exMsg, sqlException);
                        try {
                            Thread.sleep(1000 * retry);
                            connect();
                        } catch (Exception ignored) {
                        }
                    } else {
                        LOGGER.error(exMsg, sqlException);
                        throw sqlException;
                    }
                } else {
                    throw sqlException;
                }
            }
        }
    }

    public void createDatabaseIfAbsent(String location) throws SQLException {
        String databaseCreation = "CREATE DATABASE IF NOT EXISTS " + database + " COMMENT 'Database for garmadon events' LOCATION '" + location + "'";
        LOGGER.info("Create database {} if not exists", database);
        execute(databaseCreation);
    }

    protected void createTableIfNotExist(String table, MessageType schema, String location) throws SQLException {
        String hiveSchema = schema.getFields().stream().map(field -> {
            try {
                return field.getName() + " " + inferHiveType(field);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.joining(", "));

        String tableCreation = "CREATE EXTERNAL TABLE IF NOT EXISTS "
            + database + "." + table
            + "(" + hiveSchema + ")"
            + " PARTITIONED BY (day string)"
            + " STORED AS PARQUET"
            + " LOCATION '" + location + "'";
        LOGGER.info("Create table {} if not exists", table);
        execute(tableCreation);
    }

    public void createPartitionIfNotExist(String table, MessageType schema, String partition, String location) throws SQLException {
        createTableIfNotExist(table, schema, location);

        String partitionCreation = "ALTER TABLE "
            + database + "." + table
            + " ADD IF NOT EXISTS PARTITION (day='" + partition + "')"
            + " LOCATION '" + location + "/day=" + partition + "'";
        LOGGER.info("Create partition day={} on {} if not exists", partition, table);
        execute(partitionCreation);
    }

    protected String inferHiveType(Type field) throws Exception {
        String fieldHiveType;
        if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("BINARY")) {
            fieldHiveType = "string";
        } else if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("INT32")) {
            fieldHiveType = "int";
        } else if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("INT64")) {
            fieldHiveType = "bigint";
        } else if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("FLOAT")) {
            fieldHiveType = "float";
        } else if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("DOUBLE")) {
            fieldHiveType = "double";
        } else if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("BOOLEAN")) {
            fieldHiveType = "boolean";
        } else {
            throw new Exception("Unsupported Data Type: " + field.asPrimitiveType().getPrimitiveTypeName().name());
        }

        if (field.isRepetition(Type.Repetition.REPEATED)) {
            fieldHiveType = "array<" + fieldHiveType + ">";
        }

        return fieldHiveType;
    }

    public void close() throws SQLException {
        if (stmt != null) stmt.close();
        if (connection != null) connection.close();
    }

    protected Statement getStmt() {
        return stmt;
    }
}
