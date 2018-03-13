package com.criteo.hadoop.garmadon.forwarder.handler;


import com.criteo.hadoop.garmadon.forwarder.handler.junit.rules.WithEmbeddedChannel;
import com.criteo.hadoop.garmadon.forwarder.message.KafkaMessage;
import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.protobuf.InvalidProtocolBufferException;
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

public class CloseHandlerTest {

    @Rule
    public WithEmbeddedChannel channel = new WithEmbeddedChannel();

    static class TestEvent {

        byte[] bytes;

        TestEvent(int size) {
            bytes = new byte[size];
            new Random().nextBytes(bytes);
        }

        TestEvent(InputStream is) throws IOException {
            bytes = IOUtils.readFully(is, 0, false);
        }
    }

    @Before
    public void executedBeforeEach() {
        CloseHandler closeHandler = new CloseHandler();
        EventHandler eventHandler = new EventHandler();
        channel.get().pipeline().addLast(closeHandler).addLast(eventHandler);

        GarmadonSerialization.register(TestEvent.class, Integer.MAX_VALUE, event -> event.bytes, TestEvent::new);
    }

    @Test
    public void CloseHandler_should_fire_an_end_event() throws TypeMarkerException, SerializationException, InvalidProtocolBufferException {
        Header header = Header.newBuilder()
                .withHostname("hostname")
                .withApplicationID("app_id")
                .withAppAttemptID("app_attempt_id")
                .withApplicationName("app_name")
                .withContainerID("container_id")
                .withUser("user")
                .withTag(Header.Tag.YARN_APPLICATION.toString())
                .build();


        byte[] raw = ProtocolMessage.create(header.serialize(), new TestEvent(100));

        ByteBuf input = Unpooled.wrappedBuffer(raw);
        Assert.assertTrue(channel.get().writeInbound(input));
        Assert.assertTrue(channel.get().finish());

        KafkaMessage expected = new KafkaMessage("app_id", raw);
        Assert.assertEquals(expected, channel.get().readInbound());

        // Check that we sendAsync the end event
        Assert.assertNotNull(channel.get().readInbound());

        //check there is nothing more
        Assert.assertNull(channel.get().readInbound());
    }

    @Test
    public void CloseHandler_should_not_fire_an_end_event_for_non_yarn_app_tag() throws TypeMarkerException, SerializationException {
        Header header = Header.newBuilder()
                .withHostname("hostname")
                .withTag(Header.Tag.FORWARDER.toString())
                .build();

        byte[] raw = ProtocolMessage.create(header.serialize(), new TestEvent(100));

        ByteBuf input = Unpooled.wrappedBuffer(raw);
        Assert.assertTrue(channel.get().writeInbound(input));
        Assert.assertTrue(channel.get().finish());

        channel.get().readInbound();

        //check there is nothing more
        Assert.assertNull(channel.get().readInbound());
    }

    @Test
    public void CloseHandler_should_not_fire_end_event_for_null_header() {
        //check there is nothing more
        Assert.assertNull(channel.get().readInbound());
    }
}
