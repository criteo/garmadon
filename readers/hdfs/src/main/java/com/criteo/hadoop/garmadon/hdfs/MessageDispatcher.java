package com.criteo.hadoop.garmadon.hdfs;

import com.criteo.hadoop.garmadon.hdfs.writer.PartitionedWriter;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Dispatches messages to several queues that are managed by several threads
 * <p>
 * The guarantee is that events associated with the same type and on the same partition will not be concurrently processed
 */
public class MessageDispatcher<MESSAGE_KIND> {

    private final HashMap<Integer, PartitionedWriter<MESSAGE_KIND>> eventTypeToWriter = new HashMap<>();

    public void dispatch(Integer eventType, String eventName, GarmadonMessage msg) {
//        final CommittableOffset offset = value.getCommittableOffset();
//        final Counter.Child messagesWritingFailures = PrometheusMetrics.buildCounterChild(
//            PrometheusMetrics.MESSAGES_WRITING_FAILURES, eventName, offset.getPartition());
//        final Counter.Child messagesWritten = PrometheusMetrics.buildCounterChild(
//            PrometheusMetrics.MESSAGES_WRITTEN, eventName, offset.getPartition());
//
//        Gauge.Child gauge = PrometheusMetrics.buildGaugeChild(PrometheusMetrics.CURRENT_RUNNING_OFFSETS,
//            eventName, offset.getPartition());
//        gauge.set(offset.getOffset());
//
//        try {
//            writer.write(Instant.ofEpochMilli(value.getTimestamp()), offset, value.toProto());
//
//            messagesWritten.inc();
//        } catch (IOException e) {
//            // We accept losing messages every now and then, but still log failures
//            messagesWritingFailures.inc();
//            LOGGER.warn("Couldn't write a message", e);
//        }
    }

    /**
     * A queue that serves events guarantying that only one event from a given subqueue can
     * be accessed at a time (until released)
     *
     * There is one subqueue per eventType + partition
     */
    static class OneByOneMultiQueue {

        private final TreeMap<EventTypeAndPartition, SubQueue> eventTypeToMessages = new TreeMap<>(
            new EventTypeAndPartition.Comparator()
        );

        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();

        private final AtomicInteger size = new AtomicInteger(0);
        private final AtomicInteger usedSubqueues = new AtomicInteger(0);

        //shared iterator to select subqueues in round robin fashion
        private Iterator<Map.Entry<EventTypeAndPartition, SubQueue>> subQueueIterator;

        void put(Integer eventType, Integer partition, Object msg) throws InterruptedException {
            lock.lock();
            try {
                SubQueue subQueue = eventTypeToMessages.computeIfAbsent(new EventTypeAndPartition(eventType, partition), ignored -> new SubQueue());
                subQueue.put(msg);

                size.incrementAndGet();

                checkNotEmptyCondition();
            } finally {
                lock.unlock();
            }
        }

        DispatchableMessage getNextMessage() throws InterruptedException {
            Map.Entry<EventTypeAndPartition, SubQueue> subQueue = getNextNonEmptySubQueue();
            EventTypeAndPartition etp = subQueue.getKey();
            SubQueue.TakenObject obj = subQueue.getValue().take();

            size.decrementAndGet();

            return new DispatchableMessage(etp.eventType, etp.partition, obj);
        }

        /*
         * After existing this method, the subqueue cannot be used again until one if its element is taken and released
         */
        private Map.Entry<EventTypeAndPartition, SubQueue> getNextNonEmptySubQueue() throws InterruptedException {
            lock.lock();
            try {
                Map.Entry<EventTypeAndPartition, SubQueue> next = null;
                while (next == null) {

                    while (size.get() == 0 || usedSubqueues.get() == eventTypeToMessages.size()) {
                        notEmpty.await();
                    }

                    //there is at least one queue with one element,
                    //maybe the iterator is not initialized or at the end
                    if (subQueueIterator == null || !subQueueIterator.hasNext()) {
                        subQueueIterator = eventTypeToMessages.entrySet().iterator();
                    }

                    Map.Entry<EventTypeAndPartition, SubQueue> candidate = subQueueIterator.next();
                    if (!candidate.getValue().isUsed() && candidate.getValue().queue.size() > 0) {
                        next = candidate;
                        next.getValue().markUsed();
                    }
                }
                return next;

            } finally {
                lock.unlock();
            }
        }

        private void checkNotEmptyCondition() {
            lock.lock();
            try {
                if (size.get() != 0 || usedSubqueues.get() != eventTypeToMessages.size()) {
                    //wake up threads waiting for available subqueues
                    notEmpty.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * SubQueue that can be mark to track in-flight objects
         */
        class SubQueue {

            private static final int MAX_ELEMENTS = 10000;

            private volatile boolean inUse = false;
            private ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<>(MAX_ELEMENTS);

            void markUsed() {
                inUse = true;
                usedSubqueues.incrementAndGet();
            }

            void release() {
                inUse = false;
                usedSubqueues.decrementAndGet();
                checkNotEmptyCondition();
            }

            boolean isUsed() {
                return inUse;
            }

            void put(Object msg) throws InterruptedException {
                queue.put(msg);
            }

            TakenObject take() throws InterruptedException {
                return new TakenObject(queue.take());
            }

            class TakenObject {

                private Object value;

                TakenObject(Object value) {
                    this.value = value;
                }

                public Object getValue() {
                    return value;
                }

                void release() {
                    SubQueue.this.release();
                }
            }

        }

        static class EventTypeAndPartition {
            private final int eventType;
            private final int partition;

            EventTypeAndPartition(int eventType, int partition) {
                this.eventType = eventType;
                this.partition = partition;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                EventTypeAndPartition that = (EventTypeAndPartition) o;
                return eventType == that.eventType &&
                    partition == that.partition;
            }

            @Override
            public int hashCode() {
                return Objects.hash(eventType, partition);
            }

            static class Comparator implements java.util.Comparator<EventTypeAndPartition> {

                @Override
                public int compare(EventTypeAndPartition o1, EventTypeAndPartition o2) {
                    int compareEventType = Integer.compare(o1.eventType, o2.eventType);
                    if (compareEventType != 0) {
                        return compareEventType;
                    } else {
                        return Integer.compare(o1.partition, o2.partition);
                    }
                }
            }
        }

        static class DispatchableMessage {
            private final int eventType;
            private final int partition;
            private final SubQueue.TakenObject msg;

            DispatchableMessage(int eventType, int partition, SubQueue.TakenObject msg) {
                this.eventType = eventType;
                this.partition = partition;
                this.msg = msg;
            }

            public int getEventType() {
                return eventType;
            }

            public int getPartition() {
                return partition;
            }

            public SubQueue.TakenObject getMsg() {
                return msg;
            }
        }

    }


}
