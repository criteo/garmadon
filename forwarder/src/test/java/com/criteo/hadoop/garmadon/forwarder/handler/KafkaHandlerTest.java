package com.criteo.hadoop.garmadon.forwarder.handler;

import com.criteo.hadoop.garmadon.forwarder.handler.junit.rules.WithEmbeddedChannel;
import com.criteo.hadoop.garmadon.forwarder.handler.junit.rules.WithMockedKafkaService;
import com.criteo.hadoop.garmadon.forwarder.message.KafkaMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Random;

import static org.mockito.Mockito.verify;

public class KafkaHandlerTest {

    @Rule
    public WithMockedKafkaService kafkaService = new WithMockedKafkaService();

    @Rule
    public WithEmbeddedChannel channel = new WithEmbeddedChannel();

    @Before
    public void executedBeforeEach() {
        KafkaHandler kafkaHandler = new KafkaHandler(kafkaService.mock());
        channel.get().pipeline().addLast(kafkaHandler);
    }

    @Test
    public void KafkaHandler_should_send_a_producer_record_matching_the_provided_input_on_expected_topic() {
        byte[] raw = new byte[230];
        new Random().nextBytes(raw);

        KafkaMessage incomingMsg = new KafkaMessage("application_1517483736011_1079", raw);

        channel.get().writeInbound(incomingMsg);

        verify(kafkaService.mock()).sendRecordAsync("application_1517483736011_1079", raw);
    }
}
