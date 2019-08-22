package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.hdfs.monitoring.PrometheusMetrics;
import com.criteo.hadoop.garmadon.hdfs.offset.Checkpointer;
import com.criteo.hadoop.garmadon.hdfs.offset.OffsetComputer;
import com.criteo.hadoop.garmadon.protobuf.ProtoConcatenator;
import com.criteo.hadoop.garmadon.reader.Offset;
import com.google.protobuf.Message;
import io.prometheus.client.Counter;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Route messages to dedicated writers for a given MESSAGE_KIND: per day, per partition.
 *
 * @param <MESSAGE_KIND> The type of messages which will ultimately get written.
 */
public class PartitionedWriter<MESSAGE_KIND> {
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedWriter.class);

    private final Function<LocalDateTime, ExpiringWriter<MESSAGE_KIND>> writerBuilder;
    private final OffsetComputer offsetComputer;
    private final Map<Integer, Map<LocalDateTime, ExpiringWriter<MESSAGE_KIND>>> perPartitionDayWriters = new HashMap<>();
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
    public PartitionedWriter(Function<LocalDateTime, ExpiringWriter<MESSAGE_KIND>> writerBuilder,
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
    void dropPartition(int partitionId) {
        perPartitionDayWriters.remove(partitionId);
        perPartitionStartOffset.remove(partitionId);
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
    void write(Instant when, Offset offset, MESSAGE_KIND msg) {

        final Counter.Child messagesWritingFailures = PrometheusMetrics.buildCounterChild(
            PrometheusMetrics.MESSAGES_WRITING_FAILURES, eventName, offset.getPartition());
        final Counter.Child messagesWritten = PrometheusMetrics.buildCounterChild(
            PrometheusMetrics.MESSAGES_WRITTEN, eventName, offset.getPartition());

        final LocalDateTime dayStartTime = LocalDateTime.ofInstant(when.truncatedTo(ChronoUnit.DAYS), UTC_ZONE);
        final int partitionId = offset.getPartition();
        final AbstractMap.SimpleEntry<Integer, LocalDateTime> dayAndPartition = new AbstractMap.SimpleEntry<>(
            partitionId, dayStartTime);

        if (!latestMessageTimeForPartitionAndDay.containsKey(dayAndPartition) ||
            when.isAfter(latestMessageTimeForPartitionAndDay.get(dayAndPartition))) {
            latestMessageTimeForPartitionAndDay.put(dayAndPartition, when);
        }

        try {

            if (shouldSkipOffset(offset.getOffset(), partitionId)) return;

            // /!\ This line must not be switched with the offset computation as this would create empty files otherwise
            final ExpiringWriter<MESSAGE_KIND> consumer = getWriter(dayStartTime, partitionId);

            consumer.write(msg, offset);

            messagesWritten.inc();

        } catch (IOException e) {
            // We accept losing messages every now and then, but still log failures
            messagesWritingFailures.inc();
            LOGGER.warn("Couldn't write a message", e);
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
    void close() {
        possiblyCloseConsumers(ignored -> true);
    }

    /**
     * Get the starting offset for a given partition.
     *
     * @param partitionsId Id of the kafka partitions
     * @return Map with partition id =&gt; lowest offset
     * @throws IOException If the offset computation failed
     */
    Map<Integer, Long> getStartingOffsets(Collection<Integer> partitionsId) throws IOException {
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

    /**
     * Look for expiring consumers, close them and remove them from the PartitionedWriter context
     */
    void expireConsumers() {
        possiblyCloseConsumers(ExpiringWriter::isExpired);
    }

    /**
     * If a given partition has no open writer, write an empty heartbeat file. This will prevent resuming from the topic
     * beginning when a given event type has no entry.
     *
     * @param partition Partition to use for naming
     * @param offset    Offset to use for naming
     */
    void heartbeat(int partition, Offset offset) {
        final Counter.Child heartbeatsSent = PrometheusMetrics.buildCounterChild(
            PrometheusMetrics.HEARTBEATS_SENT, eventName, partition);
        PrometheusMetrics.buildCounterChild(PrometheusMetrics.MESSAGES_WRITTEN, eventName, offset.getPartition());

        try {
            if ((!perPartitionDayWriters.containsKey(partition) || perPartitionDayWriters.get(partition).isEmpty())
                && !shouldSkipOffset(offset.getOffset(), partition)) {
                final ExpiringWriter<MESSAGE_KIND> heartbeatWriter = writerBuilder.apply(LocalDateTime.now());

                MESSAGE_KIND msg = (MESSAGE_KIND) ProtoConcatenator
                    .concatToProtobuf(System.currentTimeMillis(), offset.getOffset(), Arrays.asList(emptyHeader, emptyMessageBuilder.build()))
                    .build();

                heartbeatWriter.write(msg, offset);

                final Path writtenFilePath = heartbeatWriter.close();

                if (writtenFilePath != null) {
                    heartbeatsSent.inc();
                    LOGGER.info("Written heartbeat file {}", writtenFilePath.toUri().getPath());
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Could not write heartbeat", e);
        }
    }

    private void possiblyCloseConsumers(Predicate<ExpiringWriter> shouldClose) {
        perPartitionDayWriters.forEach((partitionId, dailyWriters) ->
            dailyWriters.entrySet().removeIf(entry -> {
                final ExpiringWriter<MESSAGE_KIND> consumer = entry.getValue();
                final LocalDateTime day = entry.getKey();

                if (shouldClose.test(consumer)) {
                    if (tryExpireConsumer(consumer)) {
                        final Counter.Child filesCommitted = PrometheusMetrics.buildCounterChild(
                            PrometheusMetrics.FILES_COMMITTED, eventName);
                        final Counter.Child checkpointsFailures = PrometheusMetrics.buildCounterChild(
                            PrometheusMetrics.CHECKPOINTS_FAILURES, eventName, partitionId);
                        final Counter.Child checkpointsSuccesses = PrometheusMetrics.buildCounterChild(
                            PrometheusMetrics.CHECKPOINTS_SUCCESSES, eventName, partitionId);

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

                        return true;
                    }
                }

                return false;
            }));
    }

    private boolean tryExpireConsumer(ExpiringWriter<MESSAGE_KIND> consumer) {
        final int maxAttempts = 5;

        for (int retry = 1; retry <= maxAttempts; ++retry) {
            try {
                consumer.close();
                return true;
            } catch (IOException e) {
                String exMsg = String.format("Couldn't close writer for %s (%d/%d)", eventName, retry, maxAttempts);
                if (retry < maxAttempts) {
                    LOGGER.warn(exMsg, e);
                    try {
                        Thread.sleep(1000 * retry);
                    } catch (InterruptedException ignored) {
                    }
                } else {
                    LOGGER.error(exMsg, e);
                }
            }
        }

        throw new RuntimeException(String.format("Couldn't close writer for %s", eventName));
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
    private ExpiringWriter<MESSAGE_KIND> getWriter(LocalDateTime dayStartTime, int partitionId) {
        perPartitionDayWriters.computeIfAbsent(partitionId, ignored -> new HashMap<>());

        final Map<LocalDateTime, ExpiringWriter<MESSAGE_KIND>> partitionMap = perPartitionDayWriters.get(partitionId);

        return partitionMap.computeIfAbsent(dayStartTime, writerBuilder);
    }

}
