package com.criteo.hadoop.garmadon.forwarder.handler.junit.rules;

import com.criteo.hadoop.garmadon.forwarder.kafka.KafkaService;
import org.junit.rules.ExternalResource;
import org.mockito.Mockito;

public class WithMockedKafkaService extends ExternalResource {

    private KafkaService kafkaService;

    @Override
    protected void before() throws Throwable {
        kafkaService = Mockito.mock(KafkaService.class);
    }

    public KafkaService mock(){
        return kafkaService;
    }

}
