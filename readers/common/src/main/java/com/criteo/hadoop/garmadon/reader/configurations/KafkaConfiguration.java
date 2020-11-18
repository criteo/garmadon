package com.criteo.hadoop.garmadon.reader.configurations;

import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import java.util.Collection;
import java.util.Properties;

public class KafkaConfiguration {
    private Properties settings = new Properties();
    private String cluster = null;
    private Collection<String> topics = GarmadonReader.DEFAULT_GARMADON_TOPICS;

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

    public Collection<String> getTopics() {
        return topics;
    }

    public void setTopics(Collection<String> topics) {
        this.topics = topics;
    }
}
