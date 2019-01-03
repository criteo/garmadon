package com.criteo.hadoop.garmadon.agent;

import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The AsyncEventProcessor dequeues events, serializes them and push them to the appender
 * It starts its own thread to decouple from the application's threads.
 * This thread must be set as a daemon thread to prevent it from
 * keeping the container alive when it should be killed
 */
public class AsyncEventProcessor implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncEventProcessor.class);

    private final BlockingQueue<Message> queue;
    private final SocketAppender appender;
    private final Thread thread;

    private volatile boolean keepRunning = true;

    public AsyncEventProcessor(SocketAppender appender) {
        this(appender, 1000);
    }

    public AsyncEventProcessor(SocketAppender appender, int maxInFlight) {
        this.queue = new ArrayBlockingQueue<>(maxInFlight);
        this.appender = appender;
        this.thread = new Thread(this, "CRITEO_HADOOP_AGENT-AsyncEventProcessor");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    private static class Message {

        private final long timestamp;
        private final Header header;
        private final Object body;

        Message(long timestamp, Header header, Object body) {
            this.timestamp = timestamp;
            this.header = header;
            this.body = body;
        }
    }

    public void offer(long timestamp, Header header, Object event) {
        this.queue.offer(new Message(timestamp, header, event));
    }

    @Override
    public void run() {
        while (keepRunning) {
            try {
                Message msg = queue.poll(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    byte[] bytes = createProtocolMessage(msg.timestamp, msg.header, msg.body);
                    if (bytes != null) {
                        appender.append(bytes);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("could not process an event", e);
            }
        }
    }

    private byte[] createProtocolMessage(long timestamp, Header header, Object body) {
        byte[] bytes = null;
        try {
            bytes = ProtocolMessage.create(timestamp, header.serialize(), body);
        } catch (TypeMarkerException typeMarkerException) {
            LOGGER.error("cannot serialize event {} because a corresponding type marker does not exists", body);
        } catch (SerializationException e) {
            LOGGER.error("cannot serialize event {}: {}", body, e.getMessage());
        }
        return bytes;
    }

    public void shutdown() {
        this.keepRunning = false;
        try {
            thread.join(TimeUnit.SECONDS.toMillis(30));
            if (thread.isAlive()) {
                LOGGER.error("Cannot stop properly AsyncEventProcessor thread");
            }
        } catch (InterruptedException ignored) {
        }
    }

}
