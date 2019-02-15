package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.heuristics.configurations.DbConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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
    private static final String HEURISTIC_HELP_TABLENAME = "garmadon_heuristic_help";
    private static final String CREATE_YARN_APP_HEURISTIC_RESULT_SQL = "INSERT INTO " + HEURISTIC_RESULT_TABLENAME +
            " (yarn_app_result_id, heuristic_class, heuristic_name, severity, score, ready) " +
            "VALUES (?, ?, ?, ?, ?, ?)";
    private static final String CREATE_YARN_APP_HEURISTIC_RESULT_DETAILS_SQL = "INSERT INTO " + HEURISTIC_RESULT_DETAILS_TABLENAME +
            " (yarn_app_heuristic_result_id, name, value, details) " +
            "VALUES (?, ?, ?, ?)";
    private static final String DELETE_ALL_HEURISTIC_HELP = "DELETE FROM " + HEURISTIC_HELP_TABLENAME;
    private static final String CREATE_HEURISTIC_HELP = "INSERT INTO " + HEURISTIC_HELP_TABLENAME +
            " (heuristic_id, help_html) " +
            "VALUES (?, ?)";

    private final Connection connection;
    private final PreparedStatement createYarnAppResultStat;
    private final PreparedStatement createYarnAppResultDetailsStat;
    private final PreparedStatement createHeuristicHelp;

    public HeuristicsResultDB(DbConfiguration dbConfiguration) {
        try {
            connection = DriverManager.getConnection(dbConfiguration.getConnectionString(), dbConfiguration.getUser(), dbConfiguration.getPassword());
        } catch (SQLException ex) {
            LOGGER.error("Cannot get JDBC connection with: {}", dbConfiguration.getConnectionString(), ex);
            throw new RuntimeException(ex);
        }
        createYarnAppResultStat = prepareStatements(CREATE_YARN_APP_HEURISTIC_RESULT_SQL);
        createYarnAppResultDetailsStat = prepareStatements(CREATE_YARN_APP_HEURISTIC_RESULT_DETAILS_SQL);
        createHeuristicHelp = prepareStatements(CREATE_HEURISTIC_HELP);
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
            createYarnAppResultStat.setString(1, heuristicResult.getAppId());
            createYarnAppResultStat.setString(2, heuristicResult.getHeuristicClass().getName());
            createYarnAppResultStat.setString(3, heuristicResult.getHeuristicClass().getSimpleName() + "@" + heuristicResult.getAttemptId());
            createYarnAppResultStat.setInt(4, heuristicResult.getSeverity());
            createYarnAppResultStat.setInt(5, heuristicResult.getScore());
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

    public void updateHeuristicHelp(List<Heuristic> heuristics) {
        try {
            connection.prepareStatement(DELETE_ALL_HEURISTIC_HELP).execute();
        } catch (SQLException ex) {
            LOGGER.warn("Error deleting all in {} table", HEURISTIC_HELP_TABLENAME, ex);
            return;
        }
        for (Heuristic heuristic : heuristics) {
            String heuristicName = heuristic.getClass().getSimpleName();
            try {
                Clob clob = connection.createClob();
                clob.setString(1, heuristic.getHelp());
                createHeuristicHelp.clearParameters();
                createHeuristicHelp.setString(1, heuristicName);
                createHeuristicHelp.setClob(2, clob);
                createHeuristicHelp.executeUpdate();
            } catch (Exception ex) {
                LOGGER.warn("Error when inserting help for {}", heuristicName, ex);
            }
        }
    }
}
