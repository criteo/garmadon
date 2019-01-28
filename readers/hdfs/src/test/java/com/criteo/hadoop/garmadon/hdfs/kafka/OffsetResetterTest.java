package com.criteo.hadoop.garmadon.hdfs.kafka;

import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.writer.PartitionedWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class OffsetResetterTest {
    private static final String TOPIC = "topic";

    @Test
    public void partitionsAssignedValidOffsets() throws IOException {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final PartitionedWriter firstWriter = mock(PartitionedWriter.class);
        final PartitionedWriter secondWriter = mock(PartitionedWriter.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, mock(java.util.function.Consumer.class),
                Arrays.asList(firstWriter, secondWriter));
        final TopicPartition firstPartition = new TopicPartition(TOPIC, 1);
        final TopicPartition secondPartition = new TopicPartition(TOPIC, 2);
        final List<TopicPartition> partitions = Arrays.asList(firstPartition, secondPartition);

        final Map<Integer, Long> firstOffsets = new HashMap<>();
        firstOffsets.put(firstPartition.partition(), 10L);
        firstOffsets.put(secondPartition.partition(), 20L);
        when(firstWriter.getStartingOffsets(any())).thenReturn(firstOffsets);

        final Map<Integer, Long> secondOffsets = new HashMap<>();
        secondOffsets.put(firstPartition.partition(), 15L);
        secondOffsets.put(secondPartition.partition(), OffsetComputer.NO_OFFSET);
        when(secondWriter.getStartingOffsets(any())).thenReturn(secondOffsets);

        offsetResetter.onPartitionsAssigned(partitions);
        verify(consumer, times(1)).seek(eq(firstPartition), eq(10L));
        verify(consumer, times(1)).seekToBeginning(Collections.singleton(secondPartition));
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void partitionsAssignedCannotFetchOffset() throws IOException {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final PartitionedWriter successfulWriter = mock(PartitionedWriter.class);
        final PartitionedWriter exceptionalWriter = mock(PartitionedWriter.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, mock(java.util.function.Consumer.class),
                Arrays.asList(successfulWriter, exceptionalWriter));
        final TopicPartition partition = new TopicPartition(TOPIC, 1);
        final List<TopicPartition> partitions = Collections.singletonList(partition);

        when(successfulWriter.getStartingOffsets(any())).thenReturn(new HashMap<>());
        when(exceptionalWriter.getStartingOffsets(any())).thenThrow(new IOException("Ayo"));

        offsetResetter.onPartitionsAssigned(partitions);
        verify(consumer, times(1)).seekToBeginning(Collections.singleton(partition));
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void partitionsAssignedNoWriter() {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, mock(java.util.function.Consumer.class),
                Collections.emptyList());
        final TopicPartition partition = new TopicPartition(TOPIC, 1);
        final List<TopicPartition> partitions = Collections.singletonList(partition);

        offsetResetter.onPartitionsAssigned(partitions);
        verify(consumer, times(1)).seekToBeginning(Collections.singleton(partition));
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void partitionsAssignedNoTopic() {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, mock(java.util.function.Consumer.class),
                Collections.singleton(mock(PartitionedWriter.class)));

        offsetResetter.onPartitionsAssigned(Collections.emptyList());
        verifyZeroInteractions(consumer);
    }

    @Test
    public void partitionsRevoked() {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final PartitionedWriter writer = mock(PartitionedWriter.class);
        final java.util.function.Consumer partitionsRevokedConsumer = mock(java.util.function.Consumer.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, partitionsRevokedConsumer,
                Collections.singleton(writer));

        offsetResetter.onPartitionsRevoked(Arrays.asList(new TopicPartition(TOPIC, 1),
                new TopicPartition(TOPIC, 2)));
        verify(writer, times(1)).dropPartition(1);
        verify(writer, times(1)).dropPartition(2);
        verify(partitionsRevokedConsumer, times(1)).accept(1);
        verify(partitionsRevokedConsumer, times(1)).accept(2);
        verifyNoMoreInteractions(writer);
    }

    @Test
    public void partitionsRevokedNoTopic() {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final PartitionedWriter writer = mock(PartitionedWriter.class);
        final java.util.function.Consumer partitionsRevokedConsumer = mock(java.util.function.Consumer.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, partitionsRevokedConsumer,
                Collections.singleton(writer));

        offsetResetter.onPartitionsRevoked(Collections.emptyList());
        verifyZeroInteractions(writer);
        verifyZeroInteractions(partitionsRevokedConsumer);
    }

    @Test
    public void partitionsRevokedNoWriter() {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, mock(java.util.function.Consumer.class),
                Collections.emptyList());

        offsetResetter.onPartitionsRevoked(Collections.singleton(new TopicPartition(TOPIC, 1)));
    }
}
