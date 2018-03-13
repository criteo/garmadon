package com.criteo.hadoop.garmadon.heuristics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class HeuristicsResultDB {

    public interface Severity {
        int NONE = 0;
        int LOW = 1;
        int MODERATE = 2;
        int SEVERE = 3;
        int CRITICAL = 4;
    }

    public static void main(String[] args) {
        HeuristicsResultDB heuristicsResultDB = new HeuristicsResultDB("jdbc:mysql://192.168.56.1:3306/");
        heuristicsResultDB.createHeuristicResult(new HeuristicResult("application_42", "container_43", GCCause.class, 2, 2));
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Heuristics.class);
    private static final String GARMADON_HEURISTICS_DBNAME = "garmadon_heuristics";
    private static final String HEURISTIC_RESULT_TABLENAME = "yarn_app_heuristic_result";
    private static final String HEURISTIC_RESULT_DETAILS_TABLENAME = "yarn_app_heuristic_result_details";
    private static final String CREATE_HEURISTIC_RESULT_SQL = "CREATE TABLE IF NOT EXISTS " + HEURISTIC_RESULT_TABLENAME +
            "(id int NOT NULL AUTO_INCREMENT," +
            "yarn_app_result_id VARCHAR(50) NOT NULL," +
            "heuristic_class VARCHAR(255) NOT NULL," +
            "heuristic_name VARCHAR(128) NOT NULL," +
            "severity tinyint(2) unsigned NOT NULL," +
            "score mediumint(9) unsigned DEFAULT '0'," +
            "PRIMARY KEY(id))";
    private static final String CREATE_HEURISTIC_RESULT_DETAILS_SQL = "CREATE TABLE IF NOT EXISTS " + HEURISTIC_RESULT_DETAILS_TABLENAME +
            "(yarn_app_heuristic_result_id int NOT NULL," +
            "name VARCHAR(128) NOT NULL DEFAULT ''," +
            "value VARCHAR(255) NOT NULL DEFAULT ''," +
            "details text," +
            "PRIMARY KEY (yarn_app_heuristic_result_id, name))";
    private static final String CREATE_YARN_APP_HEURISTIC_RESULT_SQL = "INSERT INTO yarn_app_heuristic_result " +
            "(yarn_app_result_id, heuristic_class, heuristic_name, severity, score) " +
            "VALUES (?, ?, ?, ?, ?)";
    private static final String CREATE_YARN_APP_HEURISTIC_RESULT_DETAILS_SQL = "INSERT INTO yarn_app_heuristic_result_details " +
            "(yarn_app_heuristic_result_id, name, value, details) " +
            "VALUES (?, ?, ?, ?)";

    private final Connection connection;
    private final PreparedStatement createYarnAppResultStat;
    private final PreparedStatement createYarnAppResultDetailsStat;

    public HeuristicsResultDB(String connectionString) {
        try {
            connection = DriverManager.getConnection(connectionString, "root", "");
        } catch (SQLException ex) {
            LOGGER.error("Cannot get JDBC connection with: {}", connectionString, ex);
            throw new RuntimeException(ex);
        }
        if (!checkDBCreated()) {
            createDB();
        }
        if (!checkTableCreated()) {
            createTables();
        }
        createYarnAppResultStat = prepareStatements(CREATE_YARN_APP_HEURISTIC_RESULT_SQL);
        createYarnAppResultDetailsStat = prepareStatements(CREATE_YARN_APP_HEURISTIC_RESULT_DETAILS_SQL);
    }

    private boolean checkDBCreated() {
        try {
            try (ResultSet rs = connection.getMetaData().getCatalogs()) {
                while (rs.next()) {
                    String dbName = rs.getString(1);
                    if (GARMADON_HEURISTICS_DBNAME.equals(dbName)) {
                        connection.setCatalog(GARMADON_HEURISTICS_DBNAME);
                        return true;
                    }
                }
            };
        } catch (SQLException ex) {
            LOGGER.error("Error when checking {} db exists", GARMADON_HEURISTICS_DBNAME, ex);
        }
        return false;
    }

    private void createDB() {
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE DATABASE " + GARMADON_HEURISTICS_DBNAME);
            LOGGER.info("Database {} created", HEURISTIC_RESULT_DETAILS_TABLENAME);
            connection.setCatalog(GARMADON_HEURISTICS_DBNAME);
        } catch (SQLException ex) {
            LOGGER.error("Error when creating database {}", GARMADON_HEURISTICS_DBNAME);
            throw new RuntimeException(ex);
        }
    }

    private boolean checkTableCreated() {
        try {
            try (ResultSet tables = connection.getMetaData().getTables(GARMADON_HEURISTICS_DBNAME, null, null, null)) {
                Set<String> tableNames = new HashSet<>();
                while (tables.next()) {
                    tableNames.add(tables.getString(3));
                }
                return tableNames.contains(HEURISTIC_RESULT_TABLENAME) && tableNames.contains(HEURISTIC_RESULT_DETAILS_TABLENAME);
            }
        } catch (SQLException ex) {
            LOGGER.error("Error when checking tables from {}", GARMADON_HEURISTICS_DBNAME);
        }
        return false;
    }

    private void createTables() {
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(CREATE_HEURISTIC_RESULT_SQL);
            LOGGER.info("Table {} created", HEURISTIC_RESULT_TABLENAME);
            statement.executeUpdate(CREATE_HEURISTIC_RESULT_DETAILS_SQL);
            LOGGER.info("Table {} created", HEURISTIC_RESULT_DETAILS_TABLENAME);
        } catch (SQLException ex) {
            LOGGER.error("Error when creating tables for {}", GARMADON_HEURISTICS_DBNAME, ex);
            throw new RuntimeException(ex);
        }
    }

    private PreparedStatement prepareStatements(String sql) {
        try {
            return connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        } catch (SQLException ex) {
            LOGGER.error("Cannot prepare statement: {}", sql, ex);
            throw new RuntimeException(ex);
        }
    }

    public void createHeuristicResult(HeuristicResult heuristicResult) {
        int resultId = -1;
        try {
            createYarnAppResultStat.clearParameters();
            createYarnAppResultStat.setString(1, heuristicResult.appId);
            createYarnAppResultStat.setString(2, heuristicResult.heuristicClass.getName());
            createYarnAppResultStat.setString(3, heuristicResult.heuristicClass.getSimpleName());
            createYarnAppResultStat.setInt(4, heuristicResult.severity);
            createYarnAppResultStat.setInt(5, heuristicResult.score);
            createYarnAppResultStat.executeUpdate();
            try (ResultSet rsGenKey = createYarnAppResultStat.getGeneratedKeys()) {
                while (rsGenKey.next()) {
                    resultId = rsGenKey.getInt(1);
                }
            }
        } catch (SQLException ex) {
            LOGGER.warn("Error inserting into {} table", HEURISTIC_RESULT_TABLENAME, ex);
            return;
        }
        if (resultId == -1) {
            LOGGER.warn("No result id retrieve from insertion into {}", HEURISTIC_RESULT_TABLENAME);
        }
        try {
            for (int i = 0; i < heuristicResult.getDetailCount(); i++) {
                createYarnAppResultDetailsStat.clearParameters();
                HeuristicResult.HeuristicResultDetail detail = heuristicResult.getDetail(i);
                createYarnAppResultDetailsStat.setInt(1, resultId);
                createYarnAppResultDetailsStat.setString(2, detail.name);
                createYarnAppResultDetailsStat.setString(3, detail.value);
                createYarnAppResultDetailsStat.setString(4, detail.details);
                createYarnAppResultDetailsStat.executeUpdate();
            }
        } catch (SQLException ex) {
            LOGGER.warn("Error inserting into {} table", HEURISTIC_RESULT_DETAILS_TABLENAME, ex);
        }
    }

}
