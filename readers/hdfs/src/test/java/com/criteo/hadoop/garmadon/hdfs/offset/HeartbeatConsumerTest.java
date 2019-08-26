package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.hdfs.writer.AsyncWriter;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.criteo.hadoop.garmadon.reader.TopicPartitionOffset;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.time.Duration;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class HeartbeatConsumerTest {
    private static final String TOPIC = "topic";
    private static final int BASE_PARTITION = 1;
    private static final long BASE_OFFSET = 123;

    @Test(timeout = 3000)
    public void heartbeat() throws InterruptedException {
        final Offset firstPartitionFirstOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION, BASE_OFFSET);
        final Offset firstPartitionSecondOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION, BASE_OFFSET + 1);
        final Offset secondPartitionFirstOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION + 1,
            BASE_OFFSET + 11);
        final Offset secondPartitionSecondOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION + 1,
            BASE_OFFSET + 12);
        AsyncWriter<String> writer = mock(AsyncWriter.class);
        final HeartbeatConsumer hb = new HeartbeatConsumer<>(writer, Duration.ofMillis(100));

        // In-order offsets
        hb.handle(buildGarmadonMessage(firstPartitionFirstOffset));
        hb.handle(buildGarmadonMessage(firstPartitionSecondOffset));

        // Out-of-order offsets
        hb.handle(buildGarmadonMessage(secondPartitionSecondOffset));
        hb.handle(buildGarmadonMessage(secondPartitionFirstOffset));

        hb.start(mock(Thread.UncaughtExceptionHandler.class));
        Thread.sleep(1000);

        verify(writer, times(1)).heartbeat(eq(BASE_PARTITION),
            argThat(new OffsetArgumentMatcher(firstPartitionSecondOffset)));

        verify(writer, times(1)).heartbeat(eq(BASE_PARTITION + 1),
            argThat(new OffsetArgumentMatcher(secondPartitionSecondOffset)));

        hb.stop().join();
    }

    @Test
    public void differentConsecutiveHeartbeats() throws InterruptedException {
        final AsyncWriter writer = mock(AsyncWriter.class);
        final HeartbeatConsumer hb = new HeartbeatConsumer<>(writer, Duration.ofMillis(100));
        final Offset firstOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION, BASE_OFFSET);
        final Offset secondOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION + 1, BASE_OFFSET + 11);

        hb.handle(buildGarmadonMessage(firstOffset));
        hb.start(mock(Thread.UncaughtExceptionHandler.class));
        Thread.sleep(1000);
        verify(writer, times(1)).heartbeat(eq(BASE_PARTITION),
            argThat(new OffsetArgumentMatcher(firstOffset)));

        hb.handle(buildGarmadonMessage(secondOffset));
        Thread.sleep(1000);
        verify(writer, times(1)).heartbeat(eq(BASE_PARTITION + 1),
            argThat(new OffsetArgumentMatcher(secondOffset)));

        hb.stop().join();
    }

    @Test
    public void dropPartition() throws InterruptedException {
        final Offset firstPartitionOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION, BASE_OFFSET + 1);
        final Offset secondPartitionOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION + 1, BASE_OFFSET + 11);
        final AsyncWriter<String> writer = mock(AsyncWriter.class);
        final HeartbeatConsumer hb = new HeartbeatConsumer<>(writer, Duration.ofSeconds(2));

        hb.handle(buildGarmadonMessage(firstPartitionOffset));
        hb.handle(buildGarmadonMessage(secondPartitionOffset));
        hb.dropPartition(BASE_PARTITION);

        hb.start(mock(Thread.UncaughtExceptionHandler.class));

        Thread.sleep(1000);

        verify(writer, times(1)).heartbeat(eq(BASE_PARTITION + 1),
            argThat(new OffsetArgumentMatcher(secondPartitionOffset)));

        verify(writer, never()).heartbeat(eq(BASE_PARTITION), any(Offset.class));

        hb.stop().join();
    }

    @Test(timeout = 3000)
    public void noMessage() {
        final int PARTITION = 1;
        final long OFFSET = 123;
        final TopicPartitionOffset offset = new TopicPartitionOffset(TOPIC, PARTITION, OFFSET);
        AsyncWriter<String> writer = mock(AsyncWriter.class);
        final HeartbeatConsumer hb = new HeartbeatConsumer<>(writer, Duration.ofMillis(10));
        hb.handle(buildGarmadonMessage(offset));

        hb.start(mock(Thread.UncaughtExceptionHandler.class));

        verify(writer, after(100).atLeast(1)).heartbeat(eq(PARTITION),
            argThat(new OffsetArgumentMatcher(offset)));

        hb.stop().join();
    }

    @Test(timeout = 3000)
    public void writerExpirerStopWhileWaiting() throws InterruptedException {
        final HeartbeatConsumer hb = new HeartbeatConsumer<String>(mock(AsyncWriter.class),
            Duration.ofMillis(10));

        hb.start(mock(Thread.UncaughtExceptionHandler.class));
        Thread.sleep(1000);
        hb.stop().join();
    }

    private GarmadonMessage buildGarmadonMessage(Offset offset) {
        final CommittableOffset committableOffset = mock(CommittableOffset.class);
        final GarmadonMessage msgMock = mock(GarmadonMessage.class);

        when(committableOffset.getOffset()).thenReturn(offset.getOffset());
        when(committableOffset.getPartition()).thenReturn(offset.getPartition());
        when(committableOffset.getTopic()).thenReturn(offset.getTopic());

        when(msgMock.getCommittableOffset()).thenReturn(committableOffset);

        return msgMock;
    }

    private class OffsetArgumentMatcher extends ArgumentMatcher<Offset> {
        private final Offset toCompare;

        OffsetArgumentMatcher(Offset toCompare) {
            this.toCompare = toCompare;
        }

        @Override
        public boolean matches(Object o) {
            if (!(o instanceof Offset)) {
                return false;
            }

            final Offset off = (Offset) o;

            return toCompare.getOffset() == off.getOffset() && toCompare.getTopic().equals(off.getTopic()) &&
                toCompare.getPartition() == toCompare.getPartition();
        }
    }
}
