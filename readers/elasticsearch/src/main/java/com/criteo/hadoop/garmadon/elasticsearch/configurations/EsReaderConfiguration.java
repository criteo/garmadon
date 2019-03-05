package com.criteo.hadoop.garmadon.elasticsearch.configurations;

import com.criteo.hadoop.garmadon.reader.configurations.KafkaConfiguration;
import com.criteo.hadoop.garmadon.reader.configurations.PrometheusConfiguration;

public class EsReaderConfiguration {
    private ElasticsearchConfiguration elasticsearch;
    private KafkaConfiguration kafka;
    private PrometheusConfiguration prometheus;

    public ElasticsearchConfiguration getElasticsearch() {
        return elasticsearch;
    }

    public void setElasticsearch(ElasticsearchConfiguration elasticsearch) {
        this.elasticsearch = elasticsearch;
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
