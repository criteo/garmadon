package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.hdfs.monitoring.PrometheusMetrics;
import com.criteo.hadoop.garmadon.hdfs.offset.Checkpointer;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.protobuf.ProtoConcatenator;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.criteo.hadoop.garmadon.reader.helper.ReaderUtils;
import com.google.protobuf.Message;
import io.prometheus.client.Counter;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Route messages to dedicated writers for a given MESSAGE_KIND: per day, per partition.
 *
 * @param <MESSAGE_KIND> The type of messages which will ultimately get written.
 */
public class PartitionedWriter<MESSAGE_KIND> implements Closeable {
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedWriter.class);

    // A pool of worker dedicated to writing files to HDFS. Allows the reader to block for less time
    // the pool is static to avoid creating too many pools
    private static final ExecutorService CONSUMER_CLOSER_THREADS = Executors.newCachedThreadPool();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(CONSUMER_CLOSER_THREADS::shutdown));
    }

    private final BiFunction<Integer, LocalDateTime, ExpiringConsumer<MESSAGE_KIND>> writerBuilder;
    private final OffsetComputer offsetComputer;
    private final Map<Integer, Map<LocalDateTime, ExpiringConsumer<MESSAGE_KIND>>> perPartitionDayWriters = new HashMap<>();
    private final HashMap<Integer, Long> perPartitionStartOffset = new HashMap<>();
    private final String eventName;
    private final Message.Builder emptyMessageBuilder;
    private final EventHeaderProtos.Header emptyHeader = EventHeaderProtos.Header.newBuilder().build();
    private final Checkpointer checkpointer;
    private Map<AbstractMap.SimpleEntry<Integer, LocalDateTime>, Instant> latestMessageTimeForPartitionAndDay;

    /**
     * @param writerBuilder       Builds an expiring writer based on a path.
     * @param offsetComputer      Computes the first offset which should not be ignored by the PartitionedWriter when
     *                            consuming message.
     * @param eventName           Event name used for logging &amp; monitoring.
     * @param emptyMessageBuilder Empty message builder used to write heartbeat
     */
    public PartitionedWriter(BiFunction<Integer, LocalDateTime, ExpiringConsumer<MESSAGE_KIND>> writerBuilder,
                             OffsetComputer offsetComputer, String eventName, Message.Builder emptyMessageBuilder,
                             Checkpointer checkpointer) {
        this.eventName = eventName;
        this.writerBuilder = writerBuilder;
        this.offsetComputer = offsetComputer;
        this.emptyMessageBuilder = emptyMessageBuilder;
        this.checkpointer = checkpointer;
        this.latestMessageTimeForPartitionAndDay = new HashMap<>();
    }

    /**
     * Stops processing events for a given partition.
     *
     * @param partitionId The partition for which to stop processing events.
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
     * @param when   Message time, used to route to the correct file
     * @param offset Message offset, used to route to the correct file
     * @param msg    Message to be written
     * @throws IOException If the offset computation failed
     */
    public void write(Instant when, Offset offset, MESSAGE_KIND msg) throws IOException {
        final LocalDateTime dayStartTime = LocalDateTime.ofInstant(when.truncatedTo(ChronoUnit.DAYS), UTC_ZONE);
        final int partitionId = offset.getPartition();
        final AbstractMap.SimpleEntry<Integer, LocalDateTime> dayAndPartition = new AbstractMap.SimpleEntry<>(
                partitionId, dayStartTime);

        synchronized (perPartitionDayWriters) {
            if (!latestMessageTimeForPartitionAndDay.containsKey(dayAndPartition) ||
                    when.isAfter(latestMessageTimeForPartitionAndDay.get(dayAndPartition))) {
                latestMessageTimeForPartitionAndDay.put(dayAndPartition, when);
            }

            if (shouldSkipOffset(offset.getOffset(), partitionId)) return;

            // /!\ This line must not be switched with the offset computation as this would create empty files otherwise
            final ExpiringConsumer<MESSAGE_KIND> consumer = getWriter(dayStartTime, partitionId);

            consumer.write(when.toEpochMilli(), msg, offset);
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
     * @param partitionsId Id of the kafka partitions
     * @return Map with partition id =&gt; lowest offset
     * @throws IOException If the offset computation failed
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
     * @param partition Partition to use for naming
     * @param offset    Offset to use for naming
     */
    public void heartbeat(int partition, Offset offset) {
        synchronized (perPartitionDayWriters) {
            final Counter.Child heartbeatsSent = PrometheusMetrics.hearbeatsSentCounter(eventName, partition);

            try {
                if ((!perPartitionDayWriters.containsKey(partition) || perPartitionDayWriters.get(partition).isEmpty())
                        && !shouldSkipOffset(offset.getOffset(), partition)) {
                    final ExpiringConsumer<MESSAGE_KIND> heartbeatWriter = writerBuilder.apply(offset.getPartition(), LocalDateTime.now());

                    long now = System.currentTimeMillis();
                    MESSAGE_KIND msg = (MESSAGE_KIND) ProtoConcatenator
                            .concatToProtobuf(now, offset.getOffset(), Arrays.asList(emptyHeader, emptyMessageBuilder.build()))
                            .build();

                    heartbeatWriter.write(now, msg, offset);

                    final Path writtenFilePath = heartbeatWriter.close();

                    if (writtenFilePath != null) {
                        heartbeatsSent.inc();
                        LOGGER.info("Written heartbeat file {}", writtenFilePath.toUri().getPath());
                    }
                }
            } catch (IOException | SQLException e) {
                LOGGER.warn("Could not write heartbeat", e);
            }
        }
    }

    private void possiblyCloseConsumers(Predicate<ExpiringConsumer<MESSAGE_KIND>> shouldClose) {
        synchronized (perPartitionDayWriters) {

            // We create a bunch of async tasks that will try to close the consumers
            List<CompletableFuture<CloseConsumerTaskResult>> futureResults = perPartitionDayWriters.entrySet().stream().flatMap(partitionAndWriters -> {
                Integer partitionId = partitionAndWriters.getKey();
                Map<LocalDateTime, ExpiringConsumer<MESSAGE_KIND>> dailyWriters = partitionAndWriters.getValue();

                return dailyWriters.entrySet().stream().map(e -> CompletableFuture.supplyAsync(
                        new CloseConsumerTask(shouldClose, partitionId, e.getKey(), e.getValue()),
                        CONSUMER_CLOSER_THREADS
                ));
            }).collect(Collectors.toList());

            CompletableFuture
                    // We wait for all those tasks to complete
                    .allOf(futureResults.toArray(new CompletableFuture<?>[0]))
                    // Upon completion, we remove the consumers if relevant
                    // we do this only once all futures have completed
                    // to avoid race condition on the map object we modify.
                    // If an error occurred, we log it. This way we make sure not to throw to early and miss
                    // cleaning work as well as logging all errors
                    .whenComplete((ignored1, ignored2) -> futureResults.forEach(futureResult -> {
                        try {
                            CloseConsumerTaskResult result = futureResult.get();
                            // If the consumer was properly closed, remove it
                            if (result.closed) {
                                perPartitionDayWriters.get(result.partitionId).remove(result.day);
                            }
                        } catch (ExecutionException | InterruptedException e) {
                            LOGGER.error("A consumer could not be closed properly", e);
                        }
                    })).join(); // return nothing or throws if any error occurred
        }
    }

    private final static class CloseConsumerTaskResult {
        private final Integer partitionId;
        private final LocalDateTime day;

        private final boolean closed;

        private CloseConsumerTaskResult(Integer partitionId, LocalDateTime day, boolean closed) {
            this.partitionId = partitionId;
            this.day = day;
            this.closed = closed;
        }
    }

    private final class CloseConsumerTask implements Supplier<CloseConsumerTaskResult> {

        private final Predicate<ExpiringConsumer<MESSAGE_KIND>> shouldClose;
        private final Integer partitionId;
        private final LocalDateTime day;
        private final ExpiringConsumer<MESSAGE_KIND> consumer;

        private CloseConsumerTask(
                Predicate<ExpiringConsumer<MESSAGE_KIND>> shouldClose,
                Integer partitionId,
                LocalDateTime day,
                ExpiringConsumer<MESSAGE_KIND> consumer
        ) {
            this.shouldClose = shouldClose;
            this.partitionId = partitionId;
            this.day = day;
            this.consumer = consumer;
        }

        @Override
        public CloseConsumerTaskResult get() {
            boolean closed = false;
            if (shouldClose.test(consumer)) {
                if (tryExpireConsumer(consumer)) {
                    final Counter.Child filesCommitted = PrometheusMetrics.filesCommittedCounter(eventName);
                    final Counter.Child checkpointsFailures = PrometheusMetrics.checkpointFailuresCounter(eventName, partitionId);
                    final Counter.Child checkpointsSuccesses = PrometheusMetrics.checkpointSuccessesCounter(eventName, partitionId);

                    filesCommitted.inc();

                    try {
                        checkpointer.tryCheckpoint(partitionId, latestMessageTimeForPartitionAndDay.get(
                                new AbstractMap.SimpleEntry<>(partitionId, day)));
                    } catch (RuntimeException e) {
                        String msg = String.format("Failed to checkpoint partition %d, date %s, event %s",
                                partitionId, day.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                                eventName);

                        LOGGER.warn(msg, e);
                        checkpointsFailures.inc();
                    }

                    checkpointsSuccesses.inc();

                    closed = true;
                }
            }
            return new CloseConsumerTaskResult(partitionId, day, closed);
        }

    }

    private boolean tryExpireConsumer(ExpiringConsumer<MESSAGE_KIND> consumer) {
        return ReaderUtils.retryAction(() -> {
            consumer.close();
            return true;
        }, String.format("Couldn't close writer for %s", eventName), ReaderUtils.EMPTY_ACTION);
    }

    /**
     * Make sure there's a writer available to write a given message and returns it.
     * <p>
     * /!\ Not thread-safe
     *
     * @param dayStartTime Time-window start time (eg. day start if daily)
     * @param partitionId  Origin partition id
     * @return Existing or just-created consumer
     */
    private ExpiringConsumer<MESSAGE_KIND> getWriter(LocalDateTime dayStartTime, int partitionId) {
        perPartitionDayWriters.computeIfAbsent(partitionId, ignored -> new HashMap<>());

        final Map<LocalDateTime, ExpiringConsumer<MESSAGE_KIND>> partitionMap = perPartitionDayWriters.get(partitionId);

        return partitionMap.computeIfAbsent(dayStartTime, k -> writerBuilder.apply(partitionId, k));
    }

    /**
     * Poll a list of PartitionedWriter instances to make them expire if relevant.
     *
     * @param <MESSAGE_KIND> Type of messages which will ultimately get written.
     */
    public static class Expirer<MESSAGE_KIND> {
        private final Collection<PartitionedWriter<MESSAGE_KIND>> writers;
        private final TemporalAmount period;

        /**
         * @param writers Writers to watch for
         * @param period  How often the Expirer should try to expire writers
         */
        public Expirer(Collection<PartitionedWriter<MESSAGE_KIND>> writers, TemporalAmount period) {
            this.writers = writers;
            this.period = period;
        }

        public void run() {
            writers.forEach(PartitionedWriter::expireConsumers);
        }

        public void stop() {
            final RuntimeException exception = new RuntimeException("failed to stop writers");
            writers.forEach(w -> {
                try {
                    w.close();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                    exception.addSuppressed(e);
                }
            });
            if (exception.getSuppressed().length > 0) {
                throw exception;
            }
        }
    }
}
