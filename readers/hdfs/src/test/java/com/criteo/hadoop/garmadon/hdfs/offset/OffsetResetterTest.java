package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.hdfs.writer.PartitionedWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class OffsetResetterTest {
    private static final String TOPIC = "topic";

    @Test
    public void partitionsAssignedValidOffsets() throws IOException {
        Consumer<Long, String> consumer = mock(Consumer.class);
        PartitionedWriter firstWriter = mock(PartitionedWriter.class);
        PartitionedWriter secondWriter = mock(PartitionedWriter.class);
        OffsetResetter offsetResetter = new OffsetResetter<>(consumer, Arrays.asList(firstWriter, secondWriter),
                mock(HeartbeatConsumer.class));
        TopicPartition firstPartition = new TopicPartition(TOPIC, 1);
        TopicPartition secondPartition = new TopicPartition(TOPIC, 2);
        List<TopicPartition> partitions = Arrays.asList(firstPartition, secondPartition);

        when(firstWriter.getStartingOffset(firstPartition.partition())).thenReturn(10L);
        when(firstWriter.getStartingOffset(secondPartition.partition())).thenReturn(20L);

        when(secondWriter.getStartingOffset(firstPartition.partition())).thenReturn(15L);
        when(secondWriter.getStartingOffset(secondPartition.partition())).thenReturn(OffsetComputer.NO_OFFSET);

        offsetResetter.onPartitionsAssigned(partitions);
        verify(consumer, times(1)).seek(eq(firstPartition), eq(10L));
        verify(consumer, times(1)).seek(eq(secondPartition), eq(20L));
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void partitionsAssignedCannotFetchOffset() throws IOException {
        Consumer<Long, String> consumer = mock(Consumer.class);
        PartitionedWriter successfulWriter = mock(PartitionedWriter.class);
        PartitionedWriter exceptionalWriter = mock(PartitionedWriter.class);
        OffsetResetter offsetResetter = new OffsetResetter<>(consumer, Arrays.asList(successfulWriter, exceptionalWriter),
                mock(HeartbeatConsumer.class));
        TopicPartition partition = new TopicPartition(TOPIC, 1);
        List<TopicPartition> partitions = Collections.singletonList(partition);

        when(successfulWriter.getStartingOffset(partition.partition())).thenReturn(12L);
        when(exceptionalWriter.getStartingOffset(partition.partition())).thenThrow(new IOException("Ayo"));

        offsetResetter.onPartitionsAssigned(partitions);
        verify(consumer, times(1)).seekToBeginning(Collections.singleton(partition));
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void partitionsAssignedNoWriter() {
        Consumer<Long, String> consumer = mock(Consumer.class);
        OffsetResetter offsetResetter = new OffsetResetter<>(consumer, Collections.emptyList(),
                mock(HeartbeatConsumer.class));
        TopicPartition partition = new TopicPartition(TOPIC, 1);
        List<TopicPartition> partitions = Collections.singletonList(partition);

        offsetResetter.onPartitionsAssigned(partitions);
        verify(consumer, times(1)).seekToBeginning(Collections.singleton(partition));
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void partitionsAssignedNoTopic() {
        Consumer<Long, String> consumer = mock(Consumer.class);
        OffsetResetter offsetResetter = new OffsetResetter<>(consumer, Collections.singleton(mock(PartitionedWriter.class)),
                mock(HeartbeatConsumer.class));

        offsetResetter.onPartitionsAssigned(Collections.emptyList());
        verifyZeroInteractions(consumer);
    }

    @Test
    public void partitionsRevoked() {
        Consumer<Long, String> consumer = mock(Consumer.class);
        PartitionedWriter writer = mock(PartitionedWriter.class);
        HeartbeatConsumer heartbeatConsumer = mock(HeartbeatConsumer.class);
        OffsetResetter offsetResetter = new OffsetResetter<>(consumer, Collections.singleton(writer), heartbeatConsumer);

        offsetResetter.onPartitionsRevoked(Arrays.asList(new TopicPartition(TOPIC, 1), new TopicPartition(TOPIC, 2)));
        verify(writer, times(1)).dropPartition(1);
        verify(writer, times(1)).dropPartition(2);
        verify(heartbeatConsumer, times(1)).dropPartition(1);
        verify(heartbeatConsumer, times(1)).dropPartition(2);
        verifyNoMoreInteractions(writer);
    }

    @Test
    public void partitionsRevokedNoTopic() {
        Consumer<Long, String> consumer = mock(Consumer.class);
        PartitionedWriter writer = mock(PartitionedWriter.class);
        HeartbeatConsumer heartbeatConsumer = mock(HeartbeatConsumer.class);
        OffsetResetter offsetResetter = new OffsetResetter<>(consumer, Collections.singleton(writer), heartbeatConsumer);

        offsetResetter.onPartitionsRevoked(Collections.emptyList());
        verifyZeroInteractions(writer);
        verifyZeroInteractions(heartbeatConsumer);
    }

    @Test
    public void partitionsRevokedNoWriter() {
        Consumer<Long, String> consumer = mock(Consumer.class);
        OffsetResetter offsetResetter = new OffsetResetter<>(consumer, Collections.emptyList(),
                mock(HeartbeatConsumer.class));

        offsetResetter.onPartitionsRevoked(Collections.singleton(new TopicPartition(TOPIC, 1)));
    }
}
