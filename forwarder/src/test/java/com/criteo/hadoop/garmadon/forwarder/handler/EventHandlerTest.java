package com.criteo.hadoop.garmadon.forwarder.handler;


import com.criteo.hadoop.garmadon.forwarder.handler.junit.rules.WithEmbeddedChannel;
import com.criteo.hadoop.garmadon.forwarder.message.KafkaMessage;
import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import sun.misc.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static junit.framework.TestCase.assertFalse;

public class EventHandlerTest {

    @Rule
    public WithEmbeddedChannel channel = new WithEmbeddedChannel();

    @Before
    public void executedBeforeEach() {
        EventHandler eventHandler = new EventHandler();
        channel.get().pipeline().addLast(eventHandler);

        GarmadonSerialization.register(TestEvent.class, Integer.MAX_VALUE, "TestEvent", event -> event.bytes, TestEvent::new);
    }

    @Test
    public void EventHandler_should_read_event_according_top_protocol() throws TypeMarkerException, SerializationException {
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

        KafkaMessage expected = new KafkaMessage("app_id", raw);
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
