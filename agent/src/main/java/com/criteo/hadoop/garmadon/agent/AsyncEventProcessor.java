package com.criteo.hadoop.garmadon.agent;

import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.exceptions.SerializationException;
import com.criteo.hadoop.garmadon.schema.exceptions.TypeMarkerException;

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

    private static final Logger logger = Logger.getLogger(AsyncEventProcessor.class);

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

        private final Header header;
        private final Object body;

        public Message(Header header, Object body) {
            this.header = header;
            this.body = body;
        }
    }

    public void offer(Header header, Object event) {
        this.queue.offer(new Message(header, event));
    }

    @Override
    public void run() {
        while (keepRunning) {
            try {
                Message msg = queue.poll(100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    byte[] bytes = createProtocolMessage(msg.header, msg.body);
                    if (bytes != null) {
                        appender.append(bytes);
                    }
                }
            } catch (Exception e) {
                logger.error("could not process an event", e);
            }
        }
    }

    private byte[] createProtocolMessage(Header header, Object body) {
        byte[] bytes = null;
        try {
            bytes = ProtocolMessage.create(header.serialize(), body);
        } catch (TypeMarkerException typeMarkerException) {
            logger.error("cannot serialize event " + body + " because a corresponding type marker does not exists");
        } catch (SerializationException e) {
            logger.error("cannot serialize event " + body + ". " + e.getMessage());
        }
        return bytes;
    }

    public void shutdown() {
        this.keepRunning = false;
        try {
            thread.join(TimeUnit.SECONDS.toMillis(30));
            if (thread.isAlive()) {
                logger.error("Cannot stop properly AsyncEventProcessor thread");
            }
        } catch (InterruptedException e) {
        }
    }

}
