package com.criteo.hadoop.garmadon.hdfs.hive;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;

public class SimpleHiveServer {
    public static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private final Path derbyDBPath;
    private final HiveServer2 hiveServer2;
    private final int port;

    SimpleHiveServer() throws IOException {
        derbyDBPath = Files.createTempDirectory("derbyDB");

        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "jdbc:derby:;databaseName=" +
            derbyDBPath.toString() + "/derbyDB" + ";create=true");
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL, true);
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, false);

        ServerSocket s = new ServerSocket(0);
        port = s.getLocalPort();
        hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
        // Required to avoid NoSuchMethodError org.apache.hive.service.cli.operation.LogDivertAppender.setWriter
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);

        hiveServer2 = new HiveServer2();
        hiveServer2.init(hiveConf);
        s.close();
        hiveServer2.start();
    }

    public String getJdbcString() {
        return "jdbc:hive2://localhost:" + port;
    }

    public void cleanup() throws IOException {
        stop();
        FileUtils.deleteDirectory(derbyDBPath.toFile());
    }

    public void stop() {
        hiveServer2.stop();
    }
}
