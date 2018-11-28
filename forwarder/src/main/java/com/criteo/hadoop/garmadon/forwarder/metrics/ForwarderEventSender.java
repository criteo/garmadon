package com.criteo.hadoop.garmadon.forwarder.metrics;

import com.criteo.hadoop.garmadon.forwarder.kafka.KafkaService;
import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;

public class ForwarderEventSender {

    private final KafkaService kafkaService;
    private final String hostname;
    private final byte[] header;

    public ForwarderEventSender(KafkaService kafkaService, String hostname, byte[] header) {
        this.kafkaService = kafkaService;
        this.hostname = hostname;
        this.header = header;
    }

    public void sendAsync(Long timestamp, Object event) {
        try {
            kafkaService.sendRecordAsync(hostname, ProtocolMessage.create(timestamp, header, event));
        } catch (SerializationException | TypeMarkerException e) {
            PrometheusHttpMetrics.EVENTS_IN_ERROR.inc();
        }
    }
}
