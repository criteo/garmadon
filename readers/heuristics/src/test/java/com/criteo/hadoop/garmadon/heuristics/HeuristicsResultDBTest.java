package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.heuristics.configurations.DbConfiguration;
import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.MysqldConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.ScriptResolver.classPathScript;
import static com.wix.mysql.config.Charset.UTF8;
import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static com.wix.mysql.distribution.Version.v5_7_latest;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.*;

public class HeuristicsResultDBTest {
    private EmbeddedMysql mysqld;
    private MysqldConfig config;
    private String elephant = "drelephant";
    private int port;

    private DbConfiguration dbConfiguration;

    private void initDb() {
        // Start Mysql DB
        mysqld = anEmbeddedMysql(config)
            .addSchema(elephant)
            .start();

        mysqld.executeScripts(elephant, classPathScript("init.sql"));
    }

    @Before
    public void setUp() {
        try {
            // Get one free port
            ServerSocket s = new ServerSocket(0);
            port = s.getLocalPort();

            // Config Embedded Mysql
            config = aMysqldConfig(v5_7_latest)
                .withCharset(UTF8)
                .withPort(port)
                .withUser(elephant, elephant)
                .build();

            s.close();
            initDb();
        } catch (Exception e) {
            e.printStackTrace();
        }

        dbConfiguration = new DbConfiguration();
        dbConfiguration.setUser(elephant);
        dbConfiguration.setPassword(elephant);
        dbConfiguration.setConnectionString("jdbc:mysql://localhost:" + port + "/" + elephant);
    }

    @After
    public void setDown() {
        mysqld.stop();
    }

    @Test
    public void createHeuristicResult() throws SQLException {
        String appId = "appId";
        String detail = "detail";
        HeuristicsResultDB db = new HeuristicsResultDB(dbConfiguration);
        HeuristicResult heuristicResult = new HeuristicResult(appId, "attemptId", GCCause.class, 1, 1);
        heuristicResult.addDetail(detail, "value", "This is a new detail");
        db.createHeuristicResult(heuristicResult);

        ResultSet rset = db.connection.createStatement().executeQuery("SELECT  * FROM garmadon_yarn_app_heuristic_result");
        HashMap<String, String> result = new HashMap<>();
        while (rset.next()) {
            result.put(rset.getString(1), rset.getString(2));
        }
        assertEquals(appId, result.get("1"));

        rset = db.connection.createStatement().executeQuery("SELECT  * FROM garmadon_yarn_app_heuristic_result_details");
        result = new HashMap<>();
        while (rset.next()) {
            result.put(rset.getString(1), rset.getString(2));
        }
        assertEquals(detail, result.get("1"));
    }

    @Test
    public void updateHeuristicHelp() throws SQLException {
        String help = "<p>\nTest\n</p>";
        HeuristicsResultDB db = new HeuristicsResultDB(dbConfiguration);

        List<Heuristic> heuristics = new ArrayList<>();
        Heuristic heuristic = mock(Heuristic.class);
        doReturn(help).when(heuristic).getHelp();
        heuristics.add(heuristic);
        db.updateHeuristicHelp(heuristics);

        ResultSet rset = db.connection.createStatement().executeQuery("SELECT  * FROM garmadon_heuristic_help");
        HashMap<String, String> result = new HashMap<>();
        while (rset.next()) {
            result.put(rset.getString(1), rset.getString(2));
        }
        assertEquals(help, result.get(heuristic.getClass().getSimpleName()));
    }

    @Test
    public void reconnectOnFailure() {
        String appId = "appId";
        String detail = "detail";
        HeuristicsResultDB db = spy(new HeuristicsResultDB(dbConfiguration));
        HeuristicResult heuristicResult = new HeuristicResult(appId, "attemptId", GCCause.class, 1, 1);
        heuristicResult.addDetail(detail, "value", "This is a new detail");
        mysqld.stop();
        new Thread(() -> {
            initDb();
        }).start();
        db.createHeuristicResult(heuristicResult);
        verify(db, atLeast(2)).initConnectionAndStatement(dbConfiguration);
    }

    @Test(expected = RuntimeException.class)
    public void throwRuntimeExceptionIfInsertFailed() {
        String appId = "appId";
        String detail = "detail";
        HeuristicsResultDB db = new HeuristicsResultDB(dbConfiguration);
        HeuristicResult heuristicResult = new HeuristicResult(appId, "attemptId", GCCause.class, 1, 1);
        heuristicResult.addDetail(detail, "value", "This is a new detail");
        mysqld.stop();
        db.createHeuristicResult(heuristicResult);
    }
}
