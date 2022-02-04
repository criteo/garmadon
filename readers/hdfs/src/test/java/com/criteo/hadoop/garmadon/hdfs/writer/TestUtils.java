package com.criteo.hadoop.garmadon.hdfs.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;

public class TestUtils {
    public static Configuration getConfigurationUsingHadoopLocalFileSystem() {
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", LocalFileSystem.class.getCanonicalName());
        return configuration;
    }
}
