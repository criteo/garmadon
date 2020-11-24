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
    private final String port;

    SimpleHiveServer() throws IOException {
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
