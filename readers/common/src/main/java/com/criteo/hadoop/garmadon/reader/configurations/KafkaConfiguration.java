package com.criteo.hadoop.garmadon.reader.configurations;

import java.util.Properties;

public class KafkaConfiguration {
    private Properties settings = new Properties();
    private String cluster = null;

    public Properties getSettings() {
        return settings;
    }

    public void setSettings(Properties settings) {
        this.settings = settings;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
}
