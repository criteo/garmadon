package com.criteo.hadoop.garmadon.agent;

import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.criteo.hadoop.garmadon.agent.utils.AsyncTestHelper.tryDuring;
import static com.criteo.hadoop.garmadon.agent.utils.ObjectBuilderTestHelper.randomByteArray;
import static org.mockito.Mockito.*;

public class AsyncEventProcessorTest {

    SocketAppender appender;
    Header header;

    private static class TestEvent {

        private final byte[] buffer;

        public TestEvent(byte[] buffer) {
            this.buffer = buffer;
        }

    }

    @Before
    public void setUp() {
        appender = mock(SocketAppender.class);
        header = Header.newBuilder().withApplicationID(UUID.randomUUID().toString()).build(); //build a header with at least something that changes between tests
        GarmadonSerialization.register(TestEvent.class, Integer.MAX_VALUE, "TestEvent", event -> event.buffer, bytes -> new TestEvent(null));
    }

    @Test
    public void EventQueueProcessor_should_drain_queue_events_to_appender_in_order() throws TypeMarkerException, SerializationException {
        AsyncEventProcessor processor = new AsyncEventProcessor(appender);
        try {
            TestEvent t1 = new TestEvent(randomByteArray(100));
            TestEvent t2 = new TestEvent(randomByteArray(100));
            TestEvent t3 = new TestEvent(randomByteArray(100));
            TestEvent t4 = new TestEvent(randomByteArray(100));
            byte[] b1 = ProtocolMessage.create(header.serialize(), t1);
            byte[] b2 = ProtocolMessage.create(header.serialize(), t2);
            byte[] b3 = ProtocolMessage.create(header.serialize(), t3);
            byte[] b4 = ProtocolMessage.create(header.serialize(), t4);
            processor.offer(header, t1);
            processor.offer(header, t2);
            processor.offer(header, t3);
            processor.offer(header, t4);

            //since vent processor in async we wait a bit
            tryDuring(1000, () -> {
                verify(appender).append(b1);
                verify(appender).append(b2);
                verify(appender).append(b3);
                verify(appender).append(b4);
                verify(appender, never()).append(null);
            });

        } finally {
            processor.shutdown();
        }
    }

    @Test
    public void EventQueueProcessor_should_recover_from_serializer_exceptions() throws TypeMarkerException, SerializationException {
        AsyncEventProcessor processor = new AsyncEventProcessor(appender);
        try {
            TestEvent oEx = new TestEvent(null);

            processor.offer(header, oEx);
            TestEvent o = new TestEvent(randomByteArray(100));
            byte[] expectedBytes = ProtocolMessage.create(header.serialize(), o);
            processor.offer(header, o); //add an event after that we expect to be handled

            tryDuring(1000, () -> {
                verify(appender).append(expectedBytes);
            });

        } finally {
            processor.shutdown();
        }
    }

    @Test
    public void EventQueueProcessor_should_recover_from_appender_exceptions() throws TypeMarkerException, SerializationException {
        byte[] bytes = randomByteArray(100);
        TestEvent oEx = new TestEvent(bytes);
        doThrow(new RuntimeException("Exception in event queue processor")).when(appender).append(ProtocolMessage.create(header.serialize(), oEx));
        AsyncEventProcessor processor = new AsyncEventProcessor(appender);
        try {
            processor.offer(header, oEx);
            TestEvent o = new TestEvent(randomByteArray(100));
            byte[] expectedBytes = ProtocolMessage.create(header.serialize(), o);
            processor.offer(header, o); //add an event after that we expect to be handled

            tryDuring(1000, () -> {
                verify(appender).append(expectedBytes);
            });

        } finally {
            processor.shutdown();
        }
    }

}
