package com.criteo.hadoop.garmadon.heuristics.configurations;

import com.criteo.hadoop.garmadon.reader.configurations.KafkaConfiguration;
import com.criteo.hadoop.garmadon.reader.configurations.PrometheusConfiguration;

public class HeuristicsReaderConfiguration {
    private DbConfiguration db;
    private HeuristicsConfiguration heuristicsConfiguration;
    private KafkaConfiguration kafka;
    private PrometheusConfiguration prometheus;

    public DbConfiguration getDb() {
        return db;
    }

    public void setDb(DbConfiguration db) {
        this.db = db;
    }

    public HeuristicsConfiguration getHeuristicsConfiguration() {
        return heuristicsConfiguration;
    }

    public void setHeuristicsConfiguration(HeuristicsConfiguration heuristicsConfiguration) {
        this.heuristicsConfiguration = heuristicsConfiguration;
    }

    public KafkaConfiguration getKafka() {
        return kafka;
    }

    public void setKafka(KafkaConfiguration kafka) {
        this.kafka = kafka;
    }

    public PrometheusConfiguration getPrometheus() {
        return prometheus;
    }

    public void setPrometheus(PrometheusConfiguration prometheus) {
        this.prometheus = prometheus;
    }
}
