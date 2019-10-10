package com.criteo.hadoop.garmadon.hdfs.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Allow pausing and resuming all currently-assigned partitions for a given consumer once all event types agreed to
 * resume consumption. Also keeps track of the overall paused state to make sure newly-assigned partitions will also get
 * paused if they need to.
 *
 * This allows applying back-pressure on the Kafka consumer without risking to get partitions unassigned due to not
 * calling Consumer#poll frequently enough.
 */
public class PartitionsPauseStateHandler implements ConsumerRebalanceListener {
    // Paused partition -> Event class asking to pause
    private final Set<Class> pausedEvents = new HashSet<>();
    private final List<TopicPartition> currentlyAssignedPartitions = new ArrayList<>();
    private final Consumer consumer;

    /**
     * @param consumer  The consumer on which pauses and resumes will be applied
     */
    public PartitionsPauseStateHandler(Consumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        synchronized (consumer) {
            currentlyAssignedPartitions.removeAll(partitions);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        synchronized (consumer) {
            currentlyAssignedPartitions.addAll(partitions);

            if (!pausedEvents.isEmpty() && !currentlyAssignedPartitions.isEmpty()) consumer.pause(currentlyAssignedPartitions);
        }
    }

    public void pause(Class pausingEvent) {
        synchronized (consumer) {
            if (pausedEvents.isEmpty() && !currentlyAssignedPartitions.isEmpty()) consumer.pause(currentlyAssignedPartitions);

            pausedEvents.add(pausingEvent);
        }
    }

    public void resume(Class resumingEvent) {
        synchronized (consumer) {
            pausedEvents.remove(resumingEvent);

            if (pausedEvents.isEmpty() && !currentlyAssignedPartitions.isEmpty()) consumer.resume(currentlyAssignedPartitions);
        }
    }
}
