package com.criteo.hadoop.garmadon.hdfs.offset;

import com.criteo.hadoop.garmadon.hdfs.writer.PartitionedWriter;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Send periodic heartbeats to a collection of writers, only if different from previous heartbeat
 * @param <MESSAGE_KIND> Event proto type
 */
public class HeartbeatConsumer<MESSAGE_KIND> implements GarmadonReader.GarmadonMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatConsumer.class);

    private final Map<Integer, Offset> latestPartitionsOffset = new HashMap<>();
    private final Map<Integer, Offset> latestHeartbeats = new HashMap<>();

    private final TemporalAmount period;
    private final Collection<PartitionedWriter<MESSAGE_KIND>> writers;
    private Thread runningThread;

    /**
     * @param writers   Writers to send heartbeats to
     * @param period    How frequently heartbeats should be sent
     */
    public HeartbeatConsumer(Collection<PartitionedWriter<MESSAGE_KIND>> writers, TemporalAmount period) {
        this.writers = writers;
        this.period = period;
    }

    public void start(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        runningThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                synchronized (latestPartitionsOffset) {
                    for (Map.Entry<Integer, Offset> partitionOffset : latestPartitionsOffset.entrySet()) {
                        int partition = partitionOffset.getKey();
                        Offset offset = partitionOffset.getValue();
                        Offset latestHeartbeat = latestHeartbeats.get(partition);

                        if (!offset.equals(latestHeartbeat)) {
                            latestHeartbeats.put(partition, offset);
                            writers.forEach(writer -> writer.heartbeat(partition, offset));
                        }
                    }
                }

                try {
                    Thread.sleep(period.get(ChronoUnit.SECONDS) * 1000);
                } catch (InterruptedException e) {
                    LOGGER.warn("Got interrupted in between heartbeats", e);
                    break;
                }
            }
        });

        runningThread.setUncaughtExceptionHandler(uncaughtExceptionHandler);

        runningThread.start();
    }

    /**
     * Keep track of messages to send heartbeats with the latest available offset
     *
     * @param msg   The message from which to extract the offset
     */
    @Override
    public void handle(GarmadonMessage msg) {
        synchronized (latestPartitionsOffset) {
            final CommittableOffset offset = msg.getCommittableOffset();
            final Offset currentMaxOffset = latestPartitionsOffset.get(offset.getPartition());

            if (currentMaxOffset == null || offset.getOffset() > currentMaxOffset.getOffset()) {
                latestPartitionsOffset.put(offset.getPartition(), offset);
            }
        }
    }

    /**
     * Stop sending heartbeats ASAP
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
                    LOGGER.info("Exception caught while waiting for heartbeat thread to finish", e);
                }

                return null;
            });
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Stop working on a given partition
     *
     * @param partition The partition to stop working on
     */
    public void dropPartition(int partition) {
        synchronized (latestPartitionsOffset) {
            latestPartitionsOffset.remove(partition);
            latestHeartbeats.remove(partition);
        }
    }
}
