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

    private static final Logger LOGGER = LoggerFactory.getLogger(Heuristics.class);

    private static final String HEURISTIC_RESULT_TABLENAME = "garmadon_yarn_app_heuristic_result";
    private static final String HEURISTIC_RESULT_DETAILS_TABLENAME = "garmadon_yarn_app_heuristic_result_details";
    private static final String CREATE_YARN_APP_HEURISTIC_RESULT_SQL = "INSERT INTO "+ HEURISTIC_RESULT_TABLENAME +
            " (yarn_app_result_id, heuristic_class, heuristic_name, severity, score, ready) " +
            "VALUES (?, ?, ?, ?, ?, ?)";
    private static final String CREATE_YARN_APP_HEURISTIC_RESULT_DETAILS_SQL = "INSERT INTO " + HEURISTIC_RESULT_DETAILS_TABLENAME +
            " (yarn_app_heuristic_result_id, name, value, details) " +
            "VALUES (?, ?, ?, ?)";

    private final Connection connection;
    private final PreparedStatement createYarnAppResultStat;
    private final PreparedStatement createYarnAppResultDetailsStat;

    public HeuristicsResultDB(String connectionString, String user, String password) {
        try {
            connection = DriverManager.getConnection(connectionString, user, password);
        } catch (SQLException ex) {
            LOGGER.error("Cannot get JDBC connection with: {}", connectionString, ex);
            throw new RuntimeException(ex);
        }
        createYarnAppResultStat = prepareStatements(CREATE_YARN_APP_HEURISTIC_RESULT_SQL);
        createYarnAppResultDetailsStat = prepareStatements(CREATE_YARN_APP_HEURISTIC_RESULT_DETAILS_SQL);
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
            createYarnAppResultStat.setInt(6, 1);
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
