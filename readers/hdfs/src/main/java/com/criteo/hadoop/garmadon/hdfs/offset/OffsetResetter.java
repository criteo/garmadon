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

public class OffsetResetter<K, V, MessageKind> implements ConsumerRebalanceListener {
    private static final long UNKNOWN_OFFSET = OffsetComputer.NO_OFFSET;
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetResetter.class);
    private final Consumer<K, V> consumer;
    private Collection<PartitionedWriter<MessageKind>> writers;
    private HeartbeatConsumer heartbeatConsumer;

    public OffsetResetter(Consumer<K, V> consumer, Collection<PartitionedWriter<MessageKind>> writers,
                          HeartbeatConsumer heartbeatConsumer) {
        this.consumer = consumer;
        this.writers = writers;
        this.heartbeatConsumer = heartbeatConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (PartitionedWriter<MessageKind> writer : writers) {
            for (TopicPartition partition : partitions) {
                writer.dropPartition(partition.partition());
                heartbeatConsumer.dropPartition(partition.partition());
            }
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            long startingOffset = UNKNOWN_OFFSET;

            for (PartitionedWriter<MessageKind> writer : writers) {
                try {
                    long writerOffset = writer.getStartingOffset(partition.partition());

                    if (writerOffset == UNKNOWN_OFFSET)
                        break;

                    if (startingOffset == UNKNOWN_OFFSET)
                        startingOffset = writerOffset;
                    else
                        startingOffset = Math.min(startingOffset, writerOffset);
                } catch (IOException e) {
                    LOGGER.warn(String.format("Couldn't get offset for partition %d, resuming from earliest",
                            partition.partition()));
                    startingOffset = UNKNOWN_OFFSET;
                    break;
                }
            }

            if (startingOffset == UNKNOWN_OFFSET) {
                consumer.seekToBeginning(Collections.singleton(partition));
                LOGGER.warn(String.format("Resuming consumption of partition %d from the beginning. " +
                                "This should not happen unless this is the first time this app runs",
                        partition.partition()));
            }
            else {
                consumer.seek(partition, startingOffset);
                LOGGER.info(String.format("Resuming consumption of partition %d from offset %d",
                        partition.partition(), startingOffset));
            }
        }
    }
}
