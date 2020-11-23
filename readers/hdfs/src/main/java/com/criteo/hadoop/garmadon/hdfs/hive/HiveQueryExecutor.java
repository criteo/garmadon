package com.criteo.hadoop.garmadon.hdfs.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveQueryExecutor.class);
  private final String jdbcUrl;
  private final String database;
  private Connection connection;
  private Statement stmt;

  public HiveQueryExecutor(String driverName, String jdbcUrl, String database) {
    this.database = database;
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    this.jdbcUrl = jdbcUrl;
  }

  public void createDatabaseIfAbsent(String location) throws SQLException {
    String databaseCreation = "CREATE DATABASE IF NOT EXISTS " + database + " COMMENT 'Database for garmadon events' LOCATION '" + location + "'";
    LOGGER.info("Create database {} if not exists", database);
    execute(databaseCreation);
  }

  public void createTableIfNotExists(String table, String hiveSchema, String location) throws SQLException {
    String tableCreation = "CREATE EXTERNAL TABLE IF NOT EXISTS "
        + database + "." + table
        + "(" + hiveSchema + ")"
        + " PARTITIONED BY (day string)"
        + " STORED AS PARQUET"
        + " LOCATION '" + location + "'";
    LOGGER.info("Create table {} if not exists", table);
    execute(tableCreation);
  }

  public void createPartition(String table, String partition, String location) throws SQLException {
    String partitionCreation = "ALTER TABLE "
        + database + "." + table
        + " ADD IF NOT EXISTS PARTITION (day='" + partition + "')"
        + " LOCATION '" + location + "/day=" + partition + "'";
    LOGGER.info("Create partition day={} on {} if not exists", partition, table);
    execute(partitionCreation);
  }

  public void connect() throws SQLException {
    connection = DriverManager.getConnection(jdbcUrl);
    stmt = connection.createStatement();
  }

  public void close() throws SQLException {
    if (stmt != null) stmt.close();
    if (connection != null) connection.close();
  }

  public void execute(String query) throws SQLException {
    LOGGER.debug("Execute hql: {}", query);
    int maxAttempts = 5;
    for (int retry = 1; retry <= maxAttempts; ++retry) {
      try {
        stmt.execute(query);
        return;
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

  public Statement getStmt() {
    return stmt;
  }
}
