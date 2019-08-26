package com.criteo.hadoop.garmadon.hdfs.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.concurrent.CompletableFuture;

/**
 * Poll a list of PartitionedWriter instances to make them expire if relevant.
 *
 * @param <MESSAGE_KIND> Type of messages which will ultimately get written.
 */
public class Expirer<MESSAGE_KIND> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Expirer.class);

    private final AsyncWriter<MESSAGE_KIND> writer;
    private final TemporalAmount period;
    private volatile Thread runningThread;

    /**
     * @param writer Writers to watch for
     * @param period  How often the Expirer should try to expire writers
     */
    public Expirer(AsyncWriter<MESSAGE_KIND> writer, TemporalAmount period) {
        this.writer = writer;
        this.period = period;
    }

    public void start(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        runningThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                writer.expireConsumers();

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
     * @return A completable future which will complete once the expirer is properly stopped
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
            }).thenRun(() -> {
                try {
                    writer.close().get();
                } catch (Exception e) {
                    LOGGER.info("Exception caught while waiting for expirer thread to finish", e);
                }
            });
        }

        return CompletableFuture.completedFuture(null);
    }
}
