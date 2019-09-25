package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.heuristics.configurations.DbConfiguration;
import com.criteo.hadoop.garmadon.reader.helper.ReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.concurrent.Callable;

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

    protected Connection connection;

    private final DbConfiguration dbConfiguration;
    private PreparedStatement createYarnAppResultStat;
    private PreparedStatement createYarnAppResultDetailsStat;
    private PreparedStatement createHeuristicHelp;

    public HeuristicsResultDB(DbConfiguration dbConfiguration) {
        this.dbConfiguration = dbConfiguration;
        initConnectionAndStatement(dbConfiguration);
    }

    protected void initConnectionAndStatement(DbConfiguration dbConfiguration) {
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
        executeUpdate(() -> {
            createYarnAppResultStat.clearParameters();
            createYarnAppResultStat.setString(1, heuristicResult.getAppId());
            createYarnAppResultStat.setString(2, heuristicResult.getHeuristicClass().getName());
            createYarnAppResultStat.setString(3, heuristicResult.getHeuristicClass().getSimpleName() + "@" + heuristicResult.getAttemptId());
            createYarnAppResultStat.setInt(4, heuristicResult.getSeverity());
            createYarnAppResultStat.setInt(5, heuristicResult.getScore());
            createYarnAppResultStat.setInt(6, 1);
            return createYarnAppResultStat.executeUpdate();
        }, "Error inserting into " + HEURISTIC_RESULT_TABLENAME + " table");
        try {
            try (ResultSet rsGenKey = createYarnAppResultStat.getGeneratedKeys()) {
                while (rsGenKey.next()) {
                    resultId = rsGenKey.getInt(1);
                }
            }
        } catch (SQLException ex) {
            LOGGER.warn("Error reading YarnAppResult key", ex);
            return;
        }
        if (resultId == -1) {
            LOGGER.warn("No result id retrieve from insertion into {}", HEURISTIC_RESULT_TABLENAME);
        }
        for (int i = 0; i < heuristicResult.getDetailCount(); i++) {
            int finalResultId = resultId;
            int finalI = i;
            executeUpdate(() -> {
                createYarnAppResultDetailsStat.clearParameters();
                HeuristicResult.HeuristicResultDetail detail = heuristicResult.getDetail(finalI);
                createYarnAppResultDetailsStat.setInt(1, finalResultId);
                createYarnAppResultDetailsStat.setString(2, detail.name);
                createYarnAppResultDetailsStat.setString(3, detail.value);
                createYarnAppResultDetailsStat.setString(4, detail.details);
                return createYarnAppResultDetailsStat.executeUpdate();
            }, "Error inserting into " + HEURISTIC_RESULT_DETAILS_TABLENAME + " table");
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
            executeUpdate(() -> {
                Clob clob = connection.createClob();
                clob.setString(1, heuristic.getHelp());
                createHeuristicHelp.clearParameters();
                createHeuristicHelp.setString(1, heuristicName);
                createHeuristicHelp.setClob(2, clob);
                return createHeuristicHelp.executeUpdate();
            }, "Error when inserting help for " + heuristicName);
        }
    }

    public void executeUpdate(Callable action, String exceptionStr) {
        ReaderUtils.retryAction(action,
            exceptionStr, () -> {
                try {
                    initConnectionAndStatement(dbConfiguration);
                } catch (Exception ignored) {
                }
            }, 15000L);
    }
}
