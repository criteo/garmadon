package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.enums.State;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GarmadonReaderTest {
    private AtomicInteger nbEventRead;
    private MockConsumer<String, byte[]> kafkaConsumer;
    private GarmadonReader.Reader reader;
    private DataAccessEventProtos.StateEvent statEvent;

    @Before
    public void setUp() {
        nbEventRead = new AtomicInteger(0);

        kafkaConsumer = new MockConsumer(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.assign(Arrays.asList(new TopicPartition("garmadon", 0)));
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("garmadon", 0), 0L);
        kafkaConsumer.updateBeginningOffsets(beginningOffsets);

        GarmadonReader.Builder builder = GarmadonReader.Builder.stream("test");
        GarmadonReader garmadonReader = builder
                .withGroupId("test")
                .intercept(GarmadonMessageFilter.ANY.INSTANCE, this::inc)
                .build(kafkaConsumer);

        reader = garmadonReader.reader;

        statEvent = DataAccessEventProtos.StateEvent.newBuilder()
                .setState(State.BEGIN.name())
                .build();
    }

    @Test
    public void GarmadonReader_should_read_event() throws TypeMarkerException, SerializationException {
        EventHeaderProtos.Header header = EventHeaderProtos.Header.newBuilder()
                .setApplicationName("appName")
                .setApplicationId("appid")
                .setUserName("user")
                .setAppAttemptId("attempt_id")
                .setHostname("host")
                .build();

        kafkaConsumer.addRecord(new ConsumerRecord("garmadon",
                0, 0L, "test", ProtocolMessage.create(System.currentTimeMillis(), header.toByteArray(), statEvent)));

        reader.readMsg();
        assert (nbEventRead.get() == 1);
    }

    @Test
    public void GarmadonReader_should_not_fail_on_non_parsable_header() throws TypeMarkerException, SerializationException {
        DataAccessEventProtos.StateEvent statEvent = DataAccessEventProtos.StateEvent.newBuilder()
                .setState(State.BEGIN.name())
                .build();

        kafkaConsumer.addRecord(new ConsumerRecord("garmadon",
                0, 0L, "test", ProtocolMessage.create(System.currentTimeMillis(), "a".getBytes(), statEvent)));

        reader.readMsg();
        assert (nbEventRead.get() == 0);
    }

    private void inc(GarmadonMessage msg) {
        nbEventRead.incrementAndGet();
    }

}
