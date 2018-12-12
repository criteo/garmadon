package com.criteo.hadoop.garmadon.reader;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * An object that carries information about the Kafka offset
 * that has just been consumed to produce the associated GarmadonMessage
 * <p>
 * It allows offset committing either synchronously or asynchronously
 *
 * @param <K>   Kafka consumer key type
 * @param <V>   Kafka consumer value type
 */
public class CommittableOffset<K, V> implements Offset {

    private final GarmadonReader.SynchronizedConsumer<K, V> consumer;
    private final String topic;
    private final int partition;
    private final long lastProcessedOffset;

    public CommittableOffset(GarmadonReader.SynchronizedConsumer<K, V> consumer, String topic, int partition, long lastProcessedOffset) {
        this.consumer = consumer;
        this.topic = topic;
        this.partition = partition;
        this.lastProcessedOffset = lastProcessedOffset;
    }

    /**
     * Commits the offset asynchronously
     *
     * @return a future completed when the offset is committed. When
     *         several offsets are committed, the value of the future
     *         can be used to now what is the actual committed offset
     */
    public CompletableFuture<TopicPartitionOffset> commitAsync() {
        CompletableFuture<TopicPartitionOffset> promise = new CompletableFuture<>();
        this.consumer.commitAsync(offsetMap(), (offsets, exception) -> {
            if (exception != null) {
                promise.completeExceptionally(exception);
            } else {
                Map.Entry<TopicPartition, OffsetAndMetadata> entry = offsets.entrySet().iterator().next();
                TopicPartitionOffset tpo = new TopicPartitionOffset(
                        entry.getKey().topic(),
                        entry.getKey().partition(),
                        entry.getValue().offset()
                );
                promise.complete(tpo);
            }
        });
        return promise;
    }

    /**
     * Commits the offset synchronously
     */
    public void commitSync() {
        this.consumer.commitSync(offsetMap());
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return lastProcessedOffset;
    }

    private Map<TopicPartition, OffsetAndMetadata> offsetMap() {
        return Collections.singletonMap(new TopicPartition(topic, partition), new OffsetAndMetadata(lastProcessedOffset + 1));
    }
}
