package com.criteo.hadoop.garmadon.hdfs.kafka;

import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.writer.AsyncWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class OffsetResetterTest {
    private static final String TOPIC = "topic";

    @Test
    public void partitionsAssignedValidOffsets() throws IOException {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final AsyncWriter writer = mock(AsyncWriter.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, mock(java.util.function.Consumer.class), writer);
        final TopicPartition firstPartition = new TopicPartition(TOPIC, 1);
        final TopicPartition secondPartition = new TopicPartition(TOPIC, 2);
        final List<TopicPartition> partitions = Arrays.asList(firstPartition, secondPartition);

        final Map<Integer, Long> offsets = new HashMap<>();
        offsets.put(firstPartition.partition(), 10L);
        offsets.put(secondPartition.partition(), OffsetComputer.NO_OFFSET);

        when(writer.getStartingOffsets(any())).thenReturn(CompletableFuture.completedFuture(offsets));

        offsetResetter.onPartitionsAssigned(partitions);
        verify(consumer, times(1)).seek(eq(firstPartition), eq(10L));
        verify(consumer, times(1)).seekToBeginning(Collections.singleton(secondPartition));
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void partitionsAssignedCannotFetchOffset() throws IOException {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final AsyncWriter writer = mock(AsyncWriter.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, mock(java.util.function.Consumer.class), writer);
        final TopicPartition partition = new TopicPartition(TOPIC, 1);
        final List<TopicPartition> partitions = Collections.singletonList(partition);

        when(writer.getStartingOffsets(any())).thenReturn(failedFuture(new IOException("Ayo")));

        offsetResetter.onPartitionsAssigned(partitions);
        verify(consumer, times(1)).seekToBeginning(Collections.singleton(partition));
        verifyNoMoreInteractions(consumer);
    }

    private <T> CompletableFuture<T> failedFuture(Exception e) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(e);
        return future;
    }

    @Test
    public void partitionsAssignedNoTopic() {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final AsyncWriter writer = mock(AsyncWriter.class);
        when(writer.getStartingOffsets(Collections.emptyList())).thenReturn(CompletableFuture.completedFuture(Collections.EMPTY_MAP));
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, mock(java.util.function.Consumer.class), writer);

        offsetResetter.onPartitionsAssigned(Collections.emptyList());
        verifyZeroInteractions(consumer);
    }

    @Test
    public void partitionsRevoked() {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final AsyncWriter writer = mock(AsyncWriter.class);
        final java.util.function.Consumer partitionsRevokedConsumer = mock(java.util.function.Consumer.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, partitionsRevokedConsumer, writer);

        offsetResetter.onPartitionsRevoked(Arrays.asList(new TopicPartition(TOPIC, 1),
                new TopicPartition(TOPIC, 2)));
        verify(writer, times(1)).close();
        verify(writer, times(1)).dropPartition(1);
        verify(writer, times(1)).dropPartition(2);
        verify(partitionsRevokedConsumer, times(1)).accept(1);
        verify(partitionsRevokedConsumer, times(1)).accept(2);
        verifyNoMoreInteractions(writer);
    }

    @Test
    public void partitionsRevokedNoTopic() {
        final Consumer<Long, String> consumer = mock(Consumer.class);
        final AsyncWriter writer = mock(AsyncWriter.class);
        final java.util.function.Consumer partitionsRevokedConsumer = mock(java.util.function.Consumer.class);
        final OffsetResetter offsetResetter = new OffsetResetter<>(consumer, partitionsRevokedConsumer, writer);

        offsetResetter.onPartitionsRevoked(Collections.emptyList());
        verify(writer, times(1)).close();
        verifyZeroInteractions(partitionsRevokedConsumer);
    }

}
