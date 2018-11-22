package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.Offset;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Route messages to dedicated writers for a given MessageKind: per day, per partition.
 *
 * @param <MessageKind>     The type of messages which will ultimately get written.
 */
public class PartitionedWriter<MessageKind> implements Closeable {
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedWriter.class);

    private final Function<LocalDateTime, ExpiringConsumer<MessageKind>> writerBuilder;
    private final OffsetComputer offsetComputer;
    private final Map<Integer, Map<LocalDateTime, ExpiringConsumer<MessageKind>>> perPartitionDayWriters = new HashMap<>();
    private final HashMap<Integer, Long> perPartitionStartOffset = new HashMap<>();

    /**
     *
     * @param writerBuilder     Builds an expiring writer based on a path.
     * @param offsetComputer    Computes the first offset which should not be ignored by the PartitionedWriter when
     *                          consuming message.
     */
    public PartitionedWriter(Function<LocalDateTime, ExpiringConsumer<MessageKind>> writerBuilder,
                             OffsetComputer offsetComputer) {
        this.writerBuilder = writerBuilder;
        this.offsetComputer = offsetComputer;
    }

    /**
     * Stops processing events for a given partition.
     *
     * @param partitionId   The partition for which to stop processing events.
     */
    public void dropPartition(int partitionId) {
        synchronized (perPartitionDayWriters) {
            perPartitionDayWriters.keySet().remove(partitionId);
            perPartitionStartOffset.keySet().remove(partitionId);
        }
    }

    /**
     * Write a message to a dedicated file
     * File path: day.partitionId.firstMessageOffset, eg. 1987-08-13.11.101
     *
     * @param when          Message time, used to route to the correct file
     * @param offset        Message offset, used to route to the correct file
     * @param msg           Message to be written
     */
    public void write(Instant when, Offset offset, MessageKind msg) throws IOException {
        final LocalDateTime dayStartTime = LocalDateTime.ofInstant(when.truncatedTo(ChronoUnit.DAYS), UTC_ZONE);
        final int partitionId = offset.getPartition();

        synchronized (perPartitionDayWriters) {
            final long startingOffset = getStartingOffset(partitionId);

            if (offset.getOffset() <= startingOffset)
                return;

            // /!\ This line must not be switched with the offset computation as this would create empty files otherwise
            final ExpiringConsumer<MessageKind> consumer = getWriter(dayStartTime, partitionId);

            consumer.write(msg, offset);
        }
    }

    /**
     * Close all open consumers
     */
    @Override
    public void close() {
        possiblyCloseConsumers(ignored -> true);
    }

    /**
     * Get the starting offset for a given partition.
     *
     * @param partitionId
     * @return              The lowest offset for a given partition
     * @throws IOException  If the offset computation failed
     */
    public long getStartingOffset(int partitionId) throws IOException {
        synchronized (perPartitionStartOffset) {
            if (!perPartitionStartOffset.containsKey(partitionId)) {
                final long startingOffset;

                try {
                    startingOffset = offsetComputer.compute(partitionId);
                } catch (IOException e) {
                    perPartitionStartOffset.put(partitionId, -1L);
                    throw (e);
                }

                perPartitionStartOffset.put(partitionId, startingOffset);
            }

            return perPartitionStartOffset.get(partitionId);
        }
    }

    /**
     * Look for expiring consumers, close them and remove them from the PartitionedWriter context
     */
    void expireConsumers() {
        possiblyCloseConsumers(ExpiringConsumer::isExpired);
    }

    /**
     * If a given partition has no open writer, write an empty heartbeat file. This will prevent resuming from the topic
     * beginning when a given event type has no entry.
     *
     * @param partition     Partition to use for naming
     * @param offset        Offset to use for naming
     */
    public void heartbeat(int partition, Offset offset) {
        synchronized (perPartitionDayWriters) {
            if (!perPartitionDayWriters.containsKey(partition) || perPartitionDayWriters.get(partition).isEmpty()) {
                final ExpiringConsumer<MessageKind> heartbeatWriter = writerBuilder.apply(LocalDateTime.now());

                try {
                    heartbeatWriter.write(null, offset);

                    final Path writtenFilePath = heartbeatWriter.close();

                    if (writtenFilePath != null)
                        LOGGER.info("Written heartbeat file {}", writtenFilePath.toUri().getPath());
                } catch (IOException e) {
                    LOGGER.warn("Could not write heartbeat", e);
                }
            }
        }
    }

    /**
     * Poll a list of PartitionedWriter instances to make them expire if relevant. To be executed in a separate thread.
     *
     * @param <MessageKind> Type of messages which will ultimately get written.
     */
    public static class Expirer<MessageKind> implements Runnable {
        private final Collection<PartitionedWriter<MessageKind>> writers;
        private final TemporalAmount period;
        private Thread runningThread;

        /**
         * @param writers   Writers to watch for
         * @param period    How often the Expirer should try to expire writers
         */
        public Expirer(Collection<PartitionedWriter<MessageKind>> writers, TemporalAmount period) {
            this.writers = writers;
            this.period = period;
        }

        @Override
        public void run() {
            runningThread = Thread.currentThread();

            while (!runningThread.isInterrupted()) {
                writers.forEach(PartitionedWriter::expireConsumers);

                try {
                    Thread.sleep(period.get(ChronoUnit.SECONDS) * 1000);
                } catch (InterruptedException e) {
                    LOGGER.warn("Got interrupted while waiting to expire writers", e);
                    break;
                }
            }
        }

        /**
         * Notify the main loop to stop running (still need to wait for the run to finish) and close all writers
         */
        public void stop() {
            if (runningThread != null)
                runningThread.interrupt();

            writers.forEach(PartitionedWriter::close);
        }
    }

    private void possiblyCloseConsumers(Predicate<ExpiringConsumer> shouldClose) {
        synchronized (perPartitionDayWriters) {
            perPartitionDayWriters.forEach((partitionId, dailyWriters) ->
                dailyWriters.entrySet().removeIf((entry) -> {
                    final ExpiringConsumer<MessageKind> consumer = entry.getValue();

                    return shouldClose.test(consumer) && tryExpireConsumer(consumer);
                }));
        }
    }

    private boolean tryExpireConsumer(ExpiringConsumer<MessageKind> consumer) {
        try {
            consumer.close();
            return true;
        } catch (IOException e) {
            LOGGER.error("Couldn't close writer, will retry later", e);
            return false;
        }
    }

    /**
     * Make sure there's a writer available to write a given message and returns it.
     *
     * /!\ Not thread-safe
     *
     * @param dayStartTime      Time-window start time (eg. day start if daily)
     * @param partitionId       Origin partition id
     * @return                  Existing or just-created consumer
     */
    private ExpiringConsumer<MessageKind> getWriter(LocalDateTime dayStartTime, int partitionId) {
        if (!perPartitionDayWriters.containsKey(partitionId))
            perPartitionDayWriters.put(partitionId, new HashMap<>());

        final Map<LocalDateTime, ExpiringConsumer<MessageKind>> partitionMap = perPartitionDayWriters.get(partitionId);

        if (!partitionMap.containsKey(dayStartTime))
            partitionMap.put(dayStartTime, writerBuilder.apply(dayStartTime));

        return partitionMap.get(dayStartTime);
    }
}
