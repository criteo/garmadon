package com.criteo.hadoop.garmadon.hdfs.configurations;

import com.criteo.hadoop.garmadon.reader.configurations.KafkaConfiguration;
import com.criteo.hadoop.garmadon.reader.configurations.PrometheusConfiguration;

public class HdfsReaderConfiguration {
    private HdfsConfiguration hdfs;
    private KafkaConfiguration kafka;
    private PrometheusConfiguration prometheus;

    public HdfsConfiguration getHdfs() {
        return hdfs;
    }

    public void setHdfs(HdfsConfiguration hdfs) {
        this.hdfs = hdfs;
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
