package com.criteo.hadoop.garmadon.protocol;

import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ProtocolMessageTest {
    private final long timestamp = System.currentTimeMillis();

    public static class TestEvent {
        byte[] buffer;

        TestEvent(byte[] buffer) {
            this.buffer = buffer;
        }
    }

    @Before
    public void setUp() {
        GarmadonSerialization.register(TestEvent.class, Integer.MAX_VALUE, "TestEvent", event -> event.buffer, bytes -> new TestEvent(null));
    }

    @Test
    public void GarmadonMessageSerializer_should_implement_spec() throws TypeMarkerException, SerializationException {

        byte[] expectedPBytes = new byte[144]; //arbitrary length for test
        new Random().nextBytes(expectedPBytes);
        TestEvent testEvent = new TestEvent(expectedPBytes);

        byte[] headerBytes = new byte[42]; //arbitrary length for header
        new Random().nextBytes(headerBytes);

        byte[] actualBytes = ProtocolMessage.create(timestamp, headerBytes, testEvent);
        ByteBuffer buf = ByteBuffer.wrap(actualBytes);
        //first 4 bytes is the event type
        assertThat(buf.getInt(), is(Integer.MAX_VALUE));
        //next 8 bytes is timestamp event
        assertThat(buf.getLong(), is(timestamp));
        //next 4 bytes is header length
        int headerLen = buf.getInt();
        //next 4 bytes bytes is payload length
        assertThat(buf.getInt(), is(144));
        //then is header
        byte[] hBytes = new byte[headerLen];
        buf.get(hBytes);
        //then is payload
        byte[] pBytes = new byte[144];
        buf.get(pBytes);
        assertThat(pBytes, is(expectedPBytes));
    }

}
