package com.criteo.hadoop.garmadon.hdfs.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class PartitionsPauseStateHandlerTest {
    @Test
    public void pauseAndResumeSingleClass() {
        Consumer<String, String> consumer = mock(Consumer.class);
        Class clazz = String.class;
        PartitionsPauseStateHandler handler = new PartitionsPauseStateHandler(consumer);
        Collection<TopicPartition> partitions = Arrays.asList(new TopicPartition("topic", 1),
                new TopicPartition("topic", 2));

        handler.onPartitionsAssigned(partitions);
        handler.pause(clazz);
        verify(consumer, times(1)).pause(partitions);
        handler.resume(clazz);
        verify(consumer, times(1)).resume(partitions);

        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void pauseAndResumeMultipleClasses() {
        Consumer<String, String> consumer = mock(Consumer.class);
        Class firstClazz = String.class;
        Class secondClazz = Integer.class;
        PartitionsPauseStateHandler handler = new PartitionsPauseStateHandler(consumer);
        Collection<TopicPartition> partitions = Arrays.asList(new TopicPartition("topic", 1),
                new TopicPartition("topic", 2));

        handler.onPartitionsAssigned(partitions);

        // Only the first pauser should trigger a pause
        handler.pause(firstClazz);
        verify(consumer, times(1)).pause(partitions);
        handler.pause(secondClazz);
        verifyNoMoreInteractions(consumer);

        // Only the last resumer should trigger a resume
        handler.resume(secondClazz);
        verifyNoMoreInteractions(consumer);
        handler.resume(firstClazz);
        verify(consumer, times(1)).resume(partitions);

        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void assignPartitionWhenPaused() {
        Consumer<String, String> consumer = mock(Consumer.class);
        Class clazz = String.class;
        PartitionsPauseStateHandler handler = new PartitionsPauseStateHandler(consumer);
        Collection<TopicPartition> initialPartitions = Arrays.asList(new TopicPartition("topic", 1),
                new TopicPartition("topic", 2));

        handler.onPartitionsAssigned(initialPartitions);
        handler.pause(clazz);
        verify(consumer, times(1)).pause(initialPartitions);
        reset(consumer);

        TopicPartition additionalPartition = new TopicPartition("topic", 3);
        Collection<TopicPartition> finalPartitions = new ArrayList<>(initialPartitions);
        finalPartitions.add(additionalPartition);

        handler.onPartitionsAssigned(Collections.singleton(additionalPartition));
        verify(consumer, times(1)).pause(finalPartitions);
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void pauseAndResumeOnlyAssignedPartitions() {
        Consumer<String, String> consumer = mock(Consumer.class);
        Class clazz = String.class;
        PartitionsPauseStateHandler handler = new PartitionsPauseStateHandler(consumer);
        TopicPartition firstPartition = new TopicPartition("topic", 1);
        TopicPartition secondPartition = new TopicPartition("topic", 2);
        Collection<TopicPartition> initialPartitions = Arrays.asList(firstPartition, secondPartition);

        handler.onPartitionsAssigned(initialPartitions);
        handler.onPartitionsRevoked(Collections.singleton(secondPartition));
        handler.pause(clazz);
        verify(consumer, times(1)).pause(Arrays.asList(firstPartition));
        handler.resume(clazz);
        verify(consumer, times(1)).resume(Arrays.asList(firstPartition));
    }

    @Test
    public void pauseAndResumeWithNoAssignedPartition() {
        Consumer<String, String> consumer = mock(Consumer.class);
        Class clazz = String.class;
        PartitionsPauseStateHandler handler = new PartitionsPauseStateHandler(consumer);

        handler.pause(clazz);
        handler.resume(clazz);

        verifyZeroInteractions(consumer);
    }
}
