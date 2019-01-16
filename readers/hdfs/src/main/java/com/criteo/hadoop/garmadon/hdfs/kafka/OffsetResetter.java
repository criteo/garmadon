package com.criteo.hadoop.garmadon.hdfs.kafka;

import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.hdfs.writer.PartitionedWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reset consumer offsets to the highest non-consumed offset everytime partitions get assigned
 * @param <K>           Consumer key
 * @param <V>           Consumer value
 * @param <MESSAGE_KIND> Writer message type
 */
public class OffsetResetter<K, V, MESSAGE_KIND> implements ConsumerRebalanceListener {
    private static final long UNKNOWN_OFFSET = OffsetComputer.NO_OFFSET;
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetResetter.class);

    private final Consumer<K, V> consumer;
    private final Collection<PartitionedWriter<MESSAGE_KIND>> writers;
    private final java.util.function.Consumer<Integer> partitionRevokedConsumer;

    /**
     * @param consumer                      The consumer to set offset for
     * @param partitionRevokedConsumer      Called whenever a given partition access gets revoked
     * @param writers                       Writers to get the latest offset from
     */
    public OffsetResetter(Consumer<K, V> consumer, java.util.function.Consumer<Integer> partitionRevokedConsumer,
                          Collection<PartitionedWriter<MESSAGE_KIND>> writers) {
        this.consumer = consumer;
        this.partitionRevokedConsumer = partitionRevokedConsumer;
        this.writers = writers;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (PartitionedWriter<MESSAGE_KIND> writer : writers) {
            for (TopicPartition partition : partitions) {
                writer.dropPartition(partition.partition());
                partitionRevokedConsumer.accept(partition.partition());
            }
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        Map<Integer, Collection<Long>> offsetsPerPartition = new HashMap<>();

        for (PartitionedWriter<MESSAGE_KIND> writer : writers) {
            final List<Integer> partitionsId = partitions.stream()
                    .map(TopicPartition::partition)
                    .collect(Collectors.toList());

            try {
                // Batch-collect starting offsets to save HDFS calls (thereby avoiding this reader to be marked as dead
                // by Kafka)
                final Map<Integer, Long> startingOffsets = writer.getStartingOffsets(partitionsId);

                startingOffsets.forEach((part, offset) ->
                        offsetsPerPartition.computeIfAbsent(part, ignored -> new ArrayList<>()).add(offset));
            } catch (IOException e) {
                LOGGER.warn("Couldn't get offset for partitions {}, will resume from earliest",
                        partitionsId.stream().map(String::valueOf).collect(Collectors.joining(", ")), e);
                partitions.forEach(part ->
                        offsetsPerPartition.computeIfAbsent(part.partition(), ignored -> new ArrayList<>())
                        .add(UNKNOWN_OFFSET));
                // Don't break here as we need all exceptional writers to cache "unknown offset" for future queries
            }
        }

        for (TopicPartition topicPartition: partitions) {
            long startingOffset = UNKNOWN_OFFSET;
            int partition = topicPartition.partition();

            if (offsetsPerPartition.containsKey(partition)) {
                for (long offset : offsetsPerPartition.get(partition)) {
                    if (offset == UNKNOWN_OFFSET) {
                        startingOffset = UNKNOWN_OFFSET;
                        break;
                    }

                    if (startingOffset == UNKNOWN_OFFSET) startingOffset = offset;
                    else startingOffset = Math.min(startingOffset, offset);
                }
            }

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
