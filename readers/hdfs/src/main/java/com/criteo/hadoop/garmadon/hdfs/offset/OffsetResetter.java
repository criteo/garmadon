package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.hdfs.writer.PartitionedWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Reset consumer offsets to the highest non-consumed offset everytime partitions get assigned
 * @param <K>           Consumer key
 * @param <V>           Consumer value
 * @param <MessageKind> Writer message type
 */
public class OffsetResetter<K, V, MessageKind> implements ConsumerRebalanceListener {
    private static final long UNKNOWN_OFFSET = OffsetComputer.NO_OFFSET;
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetResetter.class);

    private final Consumer<K, V> consumer;
    private final Collection<PartitionedWriter<MessageKind>> writers;
    private final java.util.function.Consumer<Integer> partitionRevokedConsumer;

    /**
     * @param consumer                      The consumer to set offset for
     * @param partitionRevokedConsumer      Called whenever a given partition access gets revoked
     * @param writers                       Writers to get the latest offset from
     */
    public OffsetResetter(Consumer<K, V> consumer, java.util.function.Consumer<Integer> partitionRevokedConsumer,
                          Collection<PartitionedWriter<MessageKind>> writers) {
        this.consumer = consumer;
        this.partitionRevokedConsumer = partitionRevokedConsumer;
        this.writers = writers;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (PartitionedWriter<MessageKind> writer : writers) {
            for (TopicPartition partition : partitions) {
                writer.dropPartition(partition.partition());
                partitionRevokedConsumer.accept(partition.partition());
            }
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            long startingOffset = UNKNOWN_OFFSET;

            for (PartitionedWriter<MessageKind> writer : writers) {
                try {
                    final long writerOffset = writer.getStartingOffset(partition.partition());

                    if (writerOffset == UNKNOWN_OFFSET)
                        break;

                    if (startingOffset == UNKNOWN_OFFSET)
                        startingOffset = writerOffset;
                    else
                        startingOffset = Math.min(startingOffset, writerOffset);
                } catch (IOException e) {
                    LOGGER.warn("Couldn't get offset for partition {}, resuming from earliest", partition.partition());
                    startingOffset = UNKNOWN_OFFSET;
                    break;
                }
            }

            if (startingOffset == UNKNOWN_OFFSET) {
                consumer.seekToBeginning(Collections.singleton(partition));
                LOGGER.warn("Resuming consumption of partition {} from the beginning. " +
                            "This should not happen unless this is the first time this app runs",
                        partition.partition());
            }
            else {
                consumer.seek(partition, startingOffset);
                LOGGER.info("Resuming consumption of partition {} from offset {}",
                        partition.partition(), startingOffset);
            }
        }
    }
}
