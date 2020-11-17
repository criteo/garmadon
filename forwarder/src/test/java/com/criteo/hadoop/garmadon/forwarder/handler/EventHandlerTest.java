package com.criteo.hadoop.garmadon.forwarder.handler;


import com.criteo.hadoop.garmadon.forwarder.handler.junit.rules.WithEmbeddedChannel;
import com.criteo.hadoop.garmadon.forwarder.message.BroadCastedKafkaMessage;
import com.criteo.hadoop.garmadon.forwarder.message.KafkaMessage;
import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;


import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertFalse;

public class EventHandlerTest {

    @Rule
    public WithEmbeddedChannel channel = new WithEmbeddedChannel();

    private Set<Integer> broadcastedTypes;
    private int broadCastedType1 = 10;
    private int broadCastedType2 = 20;

    @Before
    public void executedBeforeEach() {
        broadcastedTypes = new HashSet<>();
        broadcastedTypes.add(broadCastedType1);
        broadcastedTypes.add(broadCastedType2);
        EventHandler eventHandler = new EventHandler(broadcastedTypes);
        channel.get().pipeline().addLast(eventHandler);

        GarmadonSerialization.register(TestEvent.class, Integer.MAX_VALUE, "TestEvent", event -> event.bytes, TestEvent::new);
    }

    @Test
    public void EventHandler_should_read_non_broadcasted_event() throws TypeMarkerException, SerializationException {
        Header header = Header.newBuilder()
                .withHostname("hostname")
                .withId("app_id")
                .withApplicationID("app_id")
                .withAttemptID("app_attempt_id")
                .withApplicationName("app_name")
                .withContainerID("container_id")
                .withUser("user")
                .withPid("pid")
                .build();


        byte[] raw = ProtocolMessage.create(System.currentTimeMillis(), header.serialize(), new TestEvent(100));

        ByteBuf input = Unpooled.wrappedBuffer(raw);
        Assert.assertTrue(channel.get().writeInbound(input));
        Assert.assertTrue(channel.get().finish());

        KafkaMessage expected = new KafkaMessage(raw);
        Assert.assertEquals(expected, channel.get().readInbound());
        //check there is nothing more
        Assert.assertNull(channel.get().readInbound());
    }

    @Test
    public void EventHandler_should_read_broadcasted_event() throws TypeMarkerException, SerializationException {
        GarmadonSerialization.register(TestEvent.class, broadCastedType2, "TestEvent", event -> event.bytes, TestEvent::new);

        Header header = Header.newBuilder()
            .withHostname("hostname")
            .withId("app_id")
            .withApplicationID("app_id")
            .withAttemptID("app_attempt_id")
            .withApplicationName("app_name")
            .withContainerID("container_id")
            .withUser("user")
            .withPid("pid")
            .build();


        byte[] raw = ProtocolMessage.create(System.currentTimeMillis(), header.serialize(), new TestEvent(100));

        ByteBuf input = Unpooled.wrappedBuffer(raw);
        Assert.assertTrue(channel.get().writeInbound(input));
        Assert.assertTrue(channel.get().finish());

        BroadCastedKafkaMessage expected = new BroadCastedKafkaMessage(raw);
        Assert.assertEquals(expected, channel.get().readInbound());
        //check there is nothing more
        Assert.assertNull(channel.get().readInbound());
    }

    @Test
    public void EventHandler_should_close_cnx_with_message_not_compliant_with_protocol() {
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4});
        channel.get().writeInbound(buf);

        assertFalse(channel.get().isOpen());
    }
}
