package com.criteo.hadoop.garmadon.reader.configurations;

import java.util.Properties;

public class KafkaConfiguration {
    private Properties settings = new Properties();

    public Properties getSettings() {
        return settings;
    }

    public void setSettings(Properties settings) {
        this.settings = settings;
    }
}
