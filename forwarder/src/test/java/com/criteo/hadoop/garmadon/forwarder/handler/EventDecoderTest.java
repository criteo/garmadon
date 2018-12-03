package com.criteo.hadoop.garmadon.forwarder.handler;

import com.criteo.hadoop.garmadon.forwarder.handler.junit.rules.WithEmbeddedChannel;
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

public class EventDecoderTest {

    @Rule
    public WithEmbeddedChannel channel = new WithEmbeddedChannel();

    @Before
    public void executedBeforeEach() {
        EventDecoder eventDecoder = new EventDecoder();
        channel.get().pipeline().addLast(eventDecoder);

        GarmadonSerialization.register(TestEvent.class, Integer.MAX_VALUE, "TestEvent", event -> event.bytes, TestEvent::new);
    }

    @Test
    public void EventDecoder_should_read_event_sent_as_whole() throws TypeMarkerException, SerializationException {
        Header header = Header.newBuilder()
                .withHostname("hostname")
                .withAttemptID("app_attempt_id")
                .withApplicationID("app_id")
                .withApplicationName("app_name")
                .withContainerID("container_id")
                .withUser("user")
                .withPid("pid")
                .build();


        byte[] raw = ProtocolMessage.create(System.currentTimeMillis(), header.serialize(), new TestEvent(100));

        ByteBuf input = Unpooled.wrappedBuffer(raw);
        Assert.assertTrue(channel.get().writeInbound(input));
        Assert.assertTrue(channel.get().finish());

        //check that the event is read as one piece
        Assert.assertEquals(Unpooled.wrappedBuffer(raw), channel.get().readInbound());
        //check there is nothing more
        Assert.assertNull(channel.get().readInbound());
    }

    @Test
    public void EventDecoder_should_read_event_sent_as_chunks() throws TypeMarkerException, SerializationException {
        Header header = Header.newBuilder()
                .withHostname("hostname")
                .withApplicationID("app_id")
                .withAttemptID("app_attempt_id")
                .withApplicationName("app_name")
                .withContainerID("container_id")
                .withUser("user")
                .withPid("pid")
                .build();


        byte[] raw = ProtocolMessage.create(System.currentTimeMillis(), header.serialize(), new TestEvent(100));

        ByteBuf input = Unpooled.wrappedBuffer(raw);

        Assert.assertFalse(channel.get().writeInbound(input.readBytes(2)));
        Assert.assertFalse(channel.get().writeInbound(input.readBytes(20)));
        Assert.assertTrue(channel.get().writeInbound(input.readBytes(raw.length - 22)));
        Assert.assertTrue(channel.get().finish());

        //check that the event is read as one piece
        Assert.assertEquals(Unpooled.wrappedBuffer(raw), channel.get().readInbound());
        //check there is nothing more
        Assert.assertNull(channel.get().readInbound());
    }

    @Test
    public void EventDecoder_should_read_many_events_of_different_sizes() throws TypeMarkerException, SerializationException {
        //The ReplayingDecoder uses an underlying bytebuf that
        //collects raw bytes from the wire, the reader index of this underlying buffer
        //grows.
        //Since we read integers at specific indices to get the full size of the event
        //these indices must take into account the fact that the underlying buffer's reader index changes

        Header header1 = Header.newBuilder()
                .withHostname("hostname")
                .withApplicationID("app_id_1")
                .withAttemptID("app_attempt_id")
                .withApplicationName("app_name_1")
                .withContainerID("container_id")
                .withUser("usertest")
                .withPid("pid")
                .build();

        byte[] raw1 = ProtocolMessage.create(System.currentTimeMillis(), header1.serialize(), new TestEvent(100));

        //create a message with bigger size
        Header header2 = Header.newBuilder()
                .withHostname("hostname")
                .withApplicationID("app_id_2")
                .withAttemptID("app_attempt_id")
                .withApplicationName("app_name_2")
                .withContainerID("container_id")
                .withUser("usertest")
                .withPid("pid")
                .build();

        byte[] raw2 = ProtocolMessage.create(System.currentTimeMillis(), header2.serialize(), new TestEvent(200));

        //create message with lower size
        Header header3 = Header.newBuilder()

                .withHostname("hostname")
                .withApplicationID("app_id_3")
                .withAttemptID("app_attempt_id")
                .withApplicationName("app_name_3")
                .withContainerID("container_id")
                .withUser("usertest")
                .withPid("pid")
                .build();

        byte[] raw3 = ProtocolMessage.create(System.currentTimeMillis(), header3.serialize(), new TestEvent(50));

        //sendAsync the events all at once
        Assert.assertTrue(channel.get().writeInbound(Unpooled.wrappedBuffer(raw1, raw2, raw3)));
        Assert.assertTrue(channel.get().finish());

        //check we see events appearing the one after the other
        Assert.assertEquals(Unpooled.wrappedBuffer(raw1), channel.get().readInbound());
        Assert.assertEquals(Unpooled.wrappedBuffer(raw2), channel.get().readInbound());
        Assert.assertEquals(Unpooled.wrappedBuffer(raw3), channel.get().readInbound());

        //check there is nothing more
        Assert.assertNull(channel.get().readInbound());
    }

}
