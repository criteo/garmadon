package com.criteo.hadoop.garmadon.hdfs.kafka;

import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.writer.AsyncWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Reset consumer offsets to the highest non-consumed offset everytime partitions get assigned
 *
 * @param <K>            Consumer key
 * @param <V>            Consumer value
 * @param <MESSAGE_KIND> Writer message type
 */
public class OffsetResetter<K, V, MESSAGE_KIND> implements ConsumerRebalanceListener {
    private static final long UNKNOWN_OFFSET = OffsetComputer.NO_OFFSET;
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetResetter.class);

    private final Consumer<K, V> consumer;
    private final AsyncWriter<MESSAGE_KIND> writer;
    private final java.util.function.Consumer<Integer> partitionRevokedConsumer;

    /**
     * @param consumer                 The consumer to set offset for
     * @param partitionRevokedConsumer Called whenever a given partition access gets revoked
     * @param writer                   Writer to get the latest offset from
     */
    public OffsetResetter(Consumer<K, V> consumer, java.util.function.Consumer<Integer> partitionRevokedConsumer,
                          AsyncWriter<MESSAGE_KIND> writer) {
        this.consumer = consumer;
        this.partitionRevokedConsumer = partitionRevokedConsumer;
        this.writer = writer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        writer.close();

        for (TopicPartition partition : partitions) {
            // Best case scenario, this should be no-op. If closing failed, we'll forget about this partition
            writer.dropPartition(partition.partition());
            partitionRevokedConsumer.accept(partition.partition());
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        final List<Integer> partitionsId = partitions.stream()
            .map(TopicPartition::partition)
            .collect(Collectors.toList());

        Map<Integer, Long> startingOffsets;
        try {
            startingOffsets = writer.getStartingOffsets(partitionsId).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warn("Couldn't get offset for partitions {}, will resume from earliest",
                partitionsId.stream().map(String::valueOf).collect(Collectors.joining(", ")), e);
            startingOffsets = partitionsId.stream().collect(Collectors.toMap(Function.identity(), ignored -> UNKNOWN_OFFSET));
        }

        for (TopicPartition topicPartition : partitions) {
            int partition = topicPartition.partition();
            long startingOffset = startingOffsets.getOrDefault(partition, UNKNOWN_OFFSET);

            synchronized (consumer) {
                if (startingOffset == UNKNOWN_OFFSET) {
                    consumer.seekToBeginning(Collections.singleton(topicPartition));
                    LOGGER.warn("Resuming consumption of partition {} from the beginning. " +
                            "This should not happen unless this is the first time this app runs",
                        partition);
                } else {
                    consumer.seek(topicPartition, startingOffset);
                    LOGGER.info("Resuming consumption of partition {} from offset {}",
                        partition, startingOffset);
                }
            }
        }
    }

}
