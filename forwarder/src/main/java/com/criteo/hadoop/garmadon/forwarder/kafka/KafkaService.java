package com.criteo.hadoop.garmadon.forwarder.kafka;

import com.criteo.hadoop.garmadon.forwarder.metrics.MetricsFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;

    public KafkaService(Properties properties){
        this.topic = "GARMADON";
        this.producer = new KafkaProducer<>(properties);
    }

    public void sendRecordAsync(String key, byte[] value) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);

        // TODO manage retry? and exception
        // Check batching, time and kafka config
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                MetricsFactory.eventsInError.inc();
                logger.error("Issue sending events", exception);
            }
        });
    }

    public void shutdown() {
        producer.flush();
        producer.close();
    }
}
