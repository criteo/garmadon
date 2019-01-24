package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.hdfs.monitoring.PrometheusMetrics;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.reader.Offset;
import io.prometheus.client.Counter;
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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Route messages to dedicated writers for a given MESSAGE_KIND: per day, per partition.
 *
 * @param <MESSAGE_KIND>     The type of messages which will ultimately get written.
 */
public class PartitionedWriter<MESSAGE_KIND> implements Closeable {
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedWriter.class);

    private final Function<LocalDateTime, ExpiringConsumer<MESSAGE_KIND>> writerBuilder;
    private final OffsetComputer offsetComputer;
    private final Map<Integer, Map<LocalDateTime, ExpiringConsumer<MESSAGE_KIND>>> perPartitionDayWriters = new HashMap<>();
    private final HashMap<Integer, Long> perPartitionStartOffset = new HashMap<>();
    private final String eventName;

    /**
     * @param writerBuilder     Builds an expiring writer based on a path.
     * @param offsetComputer    Computes the first offset which should not be ignored by the PartitionedWriter when
     *                          consuming message.
     * @param eventName         Event name used for logging &amp; monitoring.
     */
    public PartitionedWriter(Function<LocalDateTime, ExpiringConsumer<MESSAGE_KIND>> writerBuilder,
                             OffsetComputer offsetComputer, String eventName) {
        this.eventName = eventName;
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
            perPartitionDayWriters.remove(partitionId);
            perPartitionStartOffset.remove(partitionId);
        }
    }

    /**
     * Write a message to a dedicated file
     * File path: day.partitionId.firstMessageOffset, eg. 1987-08-13.11.101
     *
     * @param when          Message time, used to route to the correct file
     * @param offset        Message offset, used to route to the correct file
     * @param msg           Message to be written
     * @throws IOException  If the offset computation failed
     */
    public void write(Instant when, Offset offset, MESSAGE_KIND msg) throws IOException {
        final LocalDateTime dayStartTime = LocalDateTime.ofInstant(when.truncatedTo(ChronoUnit.DAYS), UTC_ZONE);
        final int partitionId = offset.getPartition();

        synchronized (perPartitionDayWriters) {
            if (shouldSkipOffset(offset.getOffset(), partitionId)) return;

            // /!\ This line must not be switched with the offset computation as this would create empty files otherwise
            final ExpiringConsumer<MESSAGE_KIND> consumer = getWriter(dayStartTime, partitionId);

            consumer.write(msg, offset);
        }
    }

    private boolean shouldSkipOffset(long offset, int partitionId) throws IOException {
        long startOffset;

        if (!perPartitionStartOffset.keySet().contains(partitionId)) {
            startOffset = getStartingOffsets(Collections.singleton(partitionId)).get(partitionId);
        } else {
            startOffset = perPartitionStartOffset.get(partitionId);
        }

        return offset <= startOffset;
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
     * @param partitionsId  Id of the kafka partitions
     * @return              Map with partition id =&gt; lowest offset
     * @throws IOException  If the offset computation failed
     */
    public Map<Integer, Long> getStartingOffsets(Collection<Integer> partitionsId) throws IOException {
        synchronized (perPartitionStartOffset) {
            if (!perPartitionStartOffset.keySet().containsAll(partitionsId)) {
                final Map<Integer, Long> startingOffsets;

                try {
                    startingOffsets = offsetComputer.computeOffsets(partitionsId);
                } catch (IOException e) {
                    partitionsId.forEach(id -> perPartitionStartOffset.put(id, OffsetComputer.NO_OFFSET));
                    throw e;
                }

                perPartitionStartOffset.putAll(startingOffsets);

                return startingOffsets;
            }

            return partitionsId.stream()
                .filter(perPartitionStartOffset::containsKey)
                .collect(Collectors.toMap(Function.identity(), perPartitionStartOffset::get));
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
            final Counter.Child heartbeatsSent = PrometheusMetrics.buildCounterChild(
                    PrometheusMetrics.HEARTBEATS_SENT, eventName, partition);
            PrometheusMetrics.buildCounterChild(PrometheusMetrics.MESSAGES_WRITTEN, eventName, offset.getPartition());

            if (!perPartitionDayWriters.containsKey(partition) || perPartitionDayWriters.get(partition).isEmpty()) {
                final ExpiringConsumer<MESSAGE_KIND> heartbeatWriter = writerBuilder.apply(LocalDateTime.now());

                try {
                    heartbeatWriter.write(null, offset);

                    final Path writtenFilePath = heartbeatWriter.close();

                    if (writtenFilePath != null) {
                        heartbeatsSent.inc();
                        LOGGER.info("Written heartbeat file {}", writtenFilePath.toUri().getPath());
                    }
                } catch (IOException e) {
                    LOGGER.warn("Could not write heartbeat", e);
                }
            }
        }
    }

    /**
     * Poll a list of PartitionedWriter instances to make them expire if relevant.
     *
     * @param <MESSAGE_KIND> Type of messages which will ultimately get written.
     */
    public static class Expirer<MESSAGE_KIND> {
        private final Collection<PartitionedWriter<MESSAGE_KIND>> writers;
        private final TemporalAmount period;
        private volatile Thread runningThread;

        /**
         * @param writers   Writers to watch for
         * @param period    How often the Expirer should try to expire writers
         */
        public Expirer(Collection<PartitionedWriter<MESSAGE_KIND>> writers, TemporalAmount period) {
            this.writers = writers;
            this.period = period;
        }

        public void start(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
            runningThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    writers.forEach(PartitionedWriter::expireConsumers);

                    try {
                        Thread.sleep(period.get(ChronoUnit.SECONDS) * 1000);
                    } catch (InterruptedException e) {
                        LOGGER.warn("Got interrupted while waiting to expire writers", e);
                        break;
                    }
                }
            });

            runningThread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
            runningThread.start();
        }

        /**
         * Notify the main loop to stop running (still need to wait for the run to finish) and close all writers
         *
         * @return  A completable future which will complete once the expirer is properly stopped
         */
        public CompletableFuture<Void> stop() {
            if (runningThread != null && runningThread.isAlive()) {
                runningThread.interrupt();

                return CompletableFuture.supplyAsync(() -> {
                    try {
                        runningThread.join();
                    } catch (InterruptedException e) {
                        LOGGER.info("Exception caught while waiting for expirer thread to finish", e);
                    }

                    return null;
                }).thenRun(() -> writers.forEach(PartitionedWriter::close));
            }

            return CompletableFuture.completedFuture(null);
        }
    }

    private void possiblyCloseConsumers(Predicate<ExpiringConsumer> shouldClose) {
        synchronized (perPartitionDayWriters) {
            perPartitionDayWriters.forEach((partitionId, dailyWriters) ->
                dailyWriters.entrySet().removeIf(entry -> {
                    final ExpiringConsumer<MESSAGE_KIND> consumer = entry.getValue();

                    if (shouldClose.test(consumer)) {
                         if (tryExpireConsumer(consumer)) {
                            final Counter.Child filesCommitted = PrometheusMetrics.buildCounterChild(
                                    PrometheusMetrics.FILES_COMMITTED, eventName);

                            filesCommitted.inc();
                            return true;
                         } else {
                             final Counter.Child filesCommitFailures = PrometheusMetrics.buildCounterChild(
                                     PrometheusMetrics.FILE_COMMIT_FAILURES, eventName);

                             filesCommitFailures.inc();
                             return false;
                         }
                    }

                    return false;
                }));
        }
    }

    private boolean tryExpireConsumer(ExpiringConsumer<MESSAGE_KIND> consumer) {
        final int maxAttempts = 3;

        for (int retry = 1; retry <= maxAttempts; ++retry) {
            try {
                consumer.close();
                return true;
            } catch (IOException e) {
                LOGGER.error("Couldn't close writer for {} ({}/{})", eventName, retry, maxAttempts, e);
            }
        }

        return false;
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
    private ExpiringConsumer<MESSAGE_KIND> getWriter(LocalDateTime dayStartTime, int partitionId) {
        perPartitionDayWriters.computeIfAbsent(partitionId, ignored -> new HashMap<>());

        final Map<LocalDateTime, ExpiringConsumer<MESSAGE_KIND>> partitionMap = perPartitionDayWriters.get(partitionId);

        return partitionMap.computeIfAbsent(dayStartTime, writerBuilder);
    }
}
