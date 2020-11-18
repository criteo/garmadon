package com.criteo.hadoop.garmadon.forwarder.kafka;

import com.criteo.hadoop.garmadon.forwarder.metrics.PrometheusHttpMetrics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaService.class);

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;

    public KafkaService(Properties properties) {
        this.topic = "garmadon";
        this.producer = new KafkaProducer<>(properties);
    }

    public void sendRecordAsync(byte[] value, boolean broadcasted) {
        if (broadcasted) {
            producer.partitionsFor(topic).forEach(partition -> {
                send(new ProducerRecord<>(topic, partition.partition(), null, value));
            });
        } else {
            send(new ProducerRecord<>(topic, value));
        }
    }

    private void send(ProducerRecord<String, byte[]> record) {
        // TODO manage retry? and exception
        // Check batching, time and kafka config
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                PrometheusHttpMetrics.EVENTS_IN_ERROR.inc();
                LOGGER.error("Issue sending events", exception);
            }
        });
    }

    public void shutdown() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}
