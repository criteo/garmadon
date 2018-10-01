package com.criteo.hadoop.garmadon.elasticsearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class UriHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(UriHelper.class);
    static Properties properties = new Properties();

    static {
        try (InputStream streamPropFilePath = UriHelper.class.getResourceAsStream("/hdfs-mapping.properties")) {
            properties.load(streamPropFilePath);
        } catch (IOException | NullPointerException e) {
            LOGGER.warn("No hdfs-mapping.properties file define");
        }
    }

    private static String concatHdfsUri(String name) {
        return "hdfs://" + name;
    }

    public static String getUniformizedUri(String uri) {
        // Remove port from uri, for.ex get only hdfs://root from hdfs://root:8020
        String[] splittedUri = uri.split(":");
        if (splittedUri.length > 2) {
            uri = splittedUri[0] + ":" + splittedUri[1];
        }

        uri = uri.replace("hdfs://", "");

        return concatHdfsUri(properties.getProperty(uri, uri));
    }

}