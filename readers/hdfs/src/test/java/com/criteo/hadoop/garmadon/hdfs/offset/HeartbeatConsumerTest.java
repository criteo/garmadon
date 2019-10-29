package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.hdfs.writer.PartitionedWriter;
import com.criteo.hadoop.garmadon.reader.*;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class HeartbeatConsumerTest {
    private static final String TOPIC = "topic";
    private static final int BASE_PARTITION = 1;
    private static final long BASE_OFFSET = 123;

    @Test(timeout = 3000)
    public void noWriter() throws InterruptedException {
        final List<PartitionedWriter<String>> writers = new ArrayList<>();
        final HeartbeatConsumer hb = new HeartbeatConsumer<>(writers, Duration.ofMillis(10));

        hb.handle(buildGarmadonMessage(new TopicPartitionOffset(TOPIC, BASE_PARTITION, BASE_OFFSET)));
        hb.run();
    }

    @Test(timeout = 3000)
    public void heartbeat() throws InterruptedException {
        final Offset firstPartitionFirstOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION, BASE_OFFSET);
        final Offset firstPartitionSecondOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION, BASE_OFFSET + 1);
        final Offset secondPartitionFirstOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION + 1,
                BASE_OFFSET + 11);
        final Offset secondPartitionSecondOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION + 1,
                BASE_OFFSET + 12);
        final List<PartitionedWriter<String>> writers = Arrays.asList(mock(PartitionedWriter.class),
                mock(PartitionedWriter.class));
        final HeartbeatConsumer hb = new HeartbeatConsumer<>(writers, Duration.ofMillis(100));

        // In-order offsets
        hb.handle(buildGarmadonMessage(firstPartitionFirstOffset));
        hb.handle(buildGarmadonMessage(firstPartitionSecondOffset));

        // Out-of-order offsets
        hb.handle(buildGarmadonMessage(secondPartitionSecondOffset));
        hb.handle(buildGarmadonMessage(secondPartitionFirstOffset));

        hb.run();

        for (PartitionedWriter<String> writer: writers) {
            verify(writer, times(1)).heartbeat(eq(BASE_PARTITION),
                    argThat(new OffsetArgumentMatcher(firstPartitionSecondOffset)));
        }

        for (PartitionedWriter<String> writer: writers) {
            verify(writer, times(1)).heartbeat(eq(BASE_PARTITION + 1),
                    argThat(new OffsetArgumentMatcher(secondPartitionSecondOffset)));
        }
    }

    @Test
    public void differentConsecutiveHeartbeats() throws InterruptedException {
        final PartitionedWriter writer = mock(PartitionedWriter.class);
        final HeartbeatConsumer hb = new HeartbeatConsumer<>(Collections.singleton(writer), Duration.ofMillis(100));
        final Offset firstOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION, BASE_OFFSET);
        final Offset secondOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION + 1, BASE_OFFSET + 11);

        hb.handle(buildGarmadonMessage(firstOffset));

        hb.run();

        verify(writer, times(1)).heartbeat(eq(BASE_PARTITION),
                argThat(new OffsetArgumentMatcher(firstOffset)));

        hb.handle(buildGarmadonMessage(secondOffset));
        hb.run();
        verify(writer, times(1)).heartbeat(eq(BASE_PARTITION + 1),
                argThat(new OffsetArgumentMatcher(secondOffset)));
    }

    @Test
    public void dropPartition() throws InterruptedException {
        final Offset firstPartitionOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION, BASE_OFFSET + 1);
        final Offset secondPartitionOffset = new TopicPartitionOffset(TOPIC, BASE_PARTITION + 1, BASE_OFFSET + 11);
        final PartitionedWriter<String> writer = mock(PartitionedWriter.class);
        final HeartbeatConsumer hb = new HeartbeatConsumer<>(Collections.singleton(writer), Duration.ofSeconds(2));

        hb.handle(buildGarmadonMessage(firstPartitionOffset));
        hb.handle(buildGarmadonMessage(secondPartitionOffset));
        hb.dropPartition(BASE_PARTITION);

        hb.run();

        Thread.sleep(1000);

        verify(writer, times(1)).heartbeat(eq(BASE_PARTITION + 1),
                argThat(new OffsetArgumentMatcher(secondPartitionOffset)));

        verify(writer, never()).heartbeat(eq(BASE_PARTITION), any(Offset.class));
    }

    @Test(timeout = 3000)
    public void noMessage() {
        final int PARTITION = 1;
        final long OFFSET = 123;
        final TopicPartitionOffset offset = new TopicPartitionOffset(TOPIC, PARTITION, OFFSET);
        final List<PartitionedWriter<String>> writers = Arrays.asList(mock(PartitionedWriter.class),
                mock(PartitionedWriter.class));
        final HeartbeatConsumer hb = new HeartbeatConsumer<>(writers, Duration.ofMillis(10));
        hb.handle(buildGarmadonMessage(offset));

        hb.run();

        verify(writers.get(0), after(100).atLeast(1)).heartbeat(eq(PARTITION),
                argThat(new OffsetArgumentMatcher(offset)));
    }

    @Test(timeout = 3000)
    public void writerExpirerStopWhileWaiting() throws InterruptedException {
        final HeartbeatConsumer hb = new HeartbeatConsumer<String>(Collections.singleton(mock(PartitionedWriter.class)),
                Duration.ofMillis(10));
        hb.run();
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
            if (!(o instanceof Offset))
                return false;

            final Offset off = (Offset) o;

            return toCompare.getOffset() == off.getOffset() && toCompare.getTopic().equals(off.getTopic()) &&
                    toCompare.getPartition() == toCompare.getPartition();
        }
    }
}
