package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.enums.State;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.hasTag;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class GarmadonReaderTest {
    private String TOPIC = "garmadon";
    private String GROUP_ID = "group_id";
    private String KEY = "key";

    //private AtomicInteger nbEventRead;
    private MockConsumer<String, byte[]> kafkaConsumer;
    private GarmadonReader.Reader reader;
    private DataAccessEventProtos.StateEvent statEvent;
    private EventHeaderProtos.Header header;
    private GarmadonReader.GarmadonMessageHandler garmadonMessageHandler;

    @Before
    public void setUp() {
        garmadonMessageHandler = mock(GarmadonReader.GarmadonMessageHandler.class);

        kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.assign(Collections.singletonList(new TopicPartition(TOPIC, 0)));
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition(TOPIC, 0), 0L);
        kafkaConsumer.updateBeginningOffsets(beginningOffsets);

        header = EventHeaderProtos.Header.newBuilder()
                .setApplicationName("appName")
                .setApplicationId("appid")
                .setUsername("user")
                .setAttemptId("attempt_id")
                .setHostname("host")
                .addTags(Header.Tag.YARN_APPLICATION.name())
                .build();

        statEvent = DataAccessEventProtos.StateEvent.newBuilder()
                .setState(State.BEGIN.name())
                .build();
    }

    private void setReader(GarmadonMessageFilter filter) {
        GarmadonReader.Builder builder = GarmadonReader.Builder.stream(kafkaConsumer);
        GarmadonReader garmadonReader = builder
                .intercept(filter, garmadonMessageHandler)
                .build(false);

        reader = garmadonReader.reader;
    }

    @Test
    public void GarmadonReader_should_read_event() throws TypeMarkerException, SerializationException {
        setReader(GarmadonMessageFilter.ANY.INSTANCE);
        kafkaConsumer.addRecord(new ConsumerRecord<>(TOPIC,
                0, 0L, KEY, ProtocolMessage.create(System.currentTimeMillis(), header.toByteArray(), statEvent)));

        reader.readConsumerRecords();
        verify(garmadonMessageHandler, times(1)).handle(any(GarmadonMessage.class));
    }

    @Test
    public void GarmadonReader_should_not_fail_on_non_parsable_header() throws TypeMarkerException, SerializationException {
        setReader(GarmadonMessageFilter.ANY.INSTANCE);
        kafkaConsumer.addRecord(new ConsumerRecord<>(TOPIC,
                0, 0L, KEY, ProtocolMessage.create(System.currentTimeMillis(), "a".getBytes(), statEvent)));

        reader.readConsumerRecords();
        verify(garmadonMessageHandler, times(0)).handle(any(GarmadonMessage.class));
    }

    @Test
    public void GarmadonReader_should_read_filtered_event() throws TypeMarkerException, SerializationException {
        setReader(hasTag(Header.Tag.YARN_APPLICATION));
        kafkaConsumer.addRecord(new ConsumerRecord<>(TOPIC,
                0, 0L, KEY, ProtocolMessage.create(System.currentTimeMillis(), header.toByteArray(), statEvent)));

        reader.readConsumerRecords();
        verify(garmadonMessageHandler, times(1)).handle(any(GarmadonMessage.class));
    }

    @Test
    public void GarmadonReader_should_not_fail_filtering_events_due_to_bad_header() throws TypeMarkerException, SerializationException {
        setReader(hasTag(Header.Tag.YARN_APPLICATION));
        kafkaConsumer.addRecord(new ConsumerRecord<>(TOPIC,
                0, 0L, KEY, ProtocolMessage.create(System.currentTimeMillis(), "a".getBytes(), statEvent)));

        reader.readConsumerRecords();
        verify(garmadonMessageHandler, times(0)).handle(any(GarmadonMessage.class));
    }

    @Test
    public void GarmadonReader_should_not_fail_if_msg_not_a_garmadon_msg() throws TypeMarkerException, SerializationException {
        setReader(hasTag(Header.Tag.YARN_APPLICATION));
        kafkaConsumer.addRecord(new ConsumerRecord<>(TOPIC,
                0, 0L, KEY, "bad msg".getBytes()));

        reader.readConsumerRecords();
        verify(garmadonMessageHandler, times(0)).handle(any(GarmadonMessage.class));
    }

    @Test
    public void GarmadonReader_should_continue_read_event_after_a_bad_event() throws TypeMarkerException, SerializationException {
        setReader(GarmadonMessageFilter.ANY.INSTANCE);
        kafkaConsumer.addRecord(new ConsumerRecord<>(TOPIC,
                0, 0L, KEY, "bad msg".getBytes()));
        kafkaConsumer.addRecord(new ConsumerRecord<>(TOPIC,
                0, 1L, KEY, ProtocolMessage.create(System.currentTimeMillis(), header.toByteArray(), statEvent)));

        reader.readConsumerRecords();
        verify(garmadonMessageHandler, times(1)).handle(any(GarmadonMessage.class));
    }

}
