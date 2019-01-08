package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import com.criteo.hadoop.garmadon.schema.exceptions.DeserializationException;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.criteo.hadoop.garmadon.protocol.ProtocolConstants.FRAME_DELIMITER_SIZE;

public final class GarmadonReader {
    public static final String GARMADON_TOPIC = "garmadon";

    public static final String CONSUMER_ID;

    private static final Logger LOGGER = LoggerFactory.getLogger(GarmadonReader.class);

    private static String hostname;

    static {
        try {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            LOGGER.error("", e);
            System.exit(1);
        }
        CONSUMER_ID = "garmadon.reader." + getHostname();

    }

    protected final Reader reader;

    private final CompletableFuture<Void> cf;

    private boolean reading = false;

    private GarmadonReader(Consumer<String, byte[]> kafkaConsumer, List<GarmadonMessageHandler> beforeInterceptHandlers,
                           Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners) {
        this.cf = new CompletableFuture<>();
        this.reader = new Reader(kafkaConsumer, beforeInterceptHandlers, listeners, cf);
    }

    public static String getHostname() {
        return hostname;
    }

    /**
     *
     * @return  A future that completes when consuming is done
     */
    public synchronized CompletableFuture<Void> startReading() {
        if (!reading) {
            new Thread(reader).start();
            reading = true;
        }
        return cf;
    }

    /**
     *
     * @return  A future that completes when consuming is done
     */
    public synchronized CompletableFuture<Void> stopReading() {
        if (reading) {
            reader.stop();
            return cf;
        } else return CompletableFuture.completedFuture(null);
    }

    protected static class Reader implements Runnable {

        private final SynchronizedConsumer<String, byte[]> consumer;
        private final CompletableFuture<Void> cf;
        private final List<GarmadonMessageHandler> beforeInterceptHandlers;
        private final Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners;
        private final Set<GarmadonMessageFilter> filters;

        private final Counter receivedCounter = new Counter();

        private volatile boolean keepOnReading = true;

        Reader(Consumer<String, byte[]> consumer, List<GarmadonMessageHandler> beforeInterceptHandlers, Map<GarmadonMessageFilter,
                GarmadonMessageHandler> listeners, CompletableFuture<Void> cf) {
            this.consumer = SynchronizedConsumer.synchronize(consumer);
            this.beforeInterceptHandlers = beforeInterceptHandlers;
            this.listeners = listeners;
            this.filters = listeners.keySet();
            this.cf = cf;
        }

        @Override
        public void run() {
            try {

                LOGGER.info("initialize reading");

                while (keepOnReading) {
                    readConsumerRecords();
                }
            } catch (Exception e) {
                LOGGER.error("unexpected exception while reading", e);
                cf.completeExceptionally(e);
            }

            LOGGER.info("stopped reading");
            cf.complete(null);

            LOGGER.info("received {} messages", receivedCounter.get());
        }

        protected void readConsumerRecords() {
            ConsumerRecords<String, byte[]> records = consumer.poll(1000L);

            for (ConsumerRecord<String, byte[]> record : records) {

                receivedCounter.increment();

                byte[] raw = record.value();
                ByteBuffer buf = ByteBuffer.wrap(raw);

                int typeMarker;
                long timestamp;
                int headerSize;
                int bodySize;
                try {
                    typeMarker = buf.getInt();
                    timestamp = buf.getLong();
                    headerSize = buf.getInt();
                    bodySize = buf.getInt();
                } catch (BufferUnderflowException e) {
                    PrometheusHttpConsumerMetrics.ISSUE_READING_GARMADON_MESSAGE_BAD_HEAD.inc();
                    LOGGER.debug("Cannot read garmadon message head for kafka record  {}", record, e);
                    continue;
                }

                int computedLength = FRAME_DELIMITER_SIZE + headerSize + bodySize;
                if (raw.length != computedLength) {
                    PrometheusHttpConsumerMetrics.ISSUE_READING_GARMADON_MESSAGE_BAD_HEAD.inc();
                    LOGGER.debug("Cannot deserialize msg due to bad computed length raw:{}, computed:{}", raw.length, computedLength);
                    continue;
                }

                EventHeaderProtos.Header header = null;
                Message body = null;

                for (GarmadonMessageFilter filter : filters) {
                    if (filter.accepts(typeMarker)) {

                        if (header == null) {
                            try {
                                header = EventHeaderProtos.Header.parseFrom(new ByteArrayInputStream(raw, FRAME_DELIMITER_SIZE, headerSize));
                            } catch (IOException e) {
                                PrometheusHttpConsumerMetrics.ISSUE_READING_PROTO_HEAD.inc();
                                LOGGER.debug("Cannot deserialize header for kafka record {} with type {}", record, typeMarker);
                                break;
                            }
                        }

                        if (filter.accepts(typeMarker, header)) {

                            if (body == null) {
                                int bodyOffset = FRAME_DELIMITER_SIZE + headerSize;
                                try {
                                    body = GarmadonSerialization.parseFrom(typeMarker, new ByteArrayInputStream(raw, bodyOffset, bodySize));
                                } catch (DeserializationException e) {
                                    PrometheusHttpConsumerMetrics.ISSUE_READING_PROTO_BODY.inc();
                                    LOGGER.debug("Cannot deserialize event from kafka record {} with type {}", record, typeMarker);
                                    break;
                                }
                            }

                            if (header != null && body != null) {
                                CommittableOffset<String, byte[]> committableOffset = new CommittableOffset<>(consumer, record.topic(),
                                        record.partition(), record.offset());

                                GarmadonMessage msg = new GarmadonMessage(typeMarker, timestamp, header, body, committableOffset);

                                beforeInterceptHandlers.forEach(c -> c.handle(msg));
                                listeners.get(filter).handle(msg);
                            }
                        }
                    }
                }
            }
        }

        void stop() {
            keepOnReading = false;
        }

        private static class Counter {

            private int count;

            void increment() {
                count++;
                if (count % 500 == 0) {
                    LOGGER.debug("Received {} messages so far", count);
                }
            }

            int get() {
                return count;
            }
        }
    }

    static final class SynchronizedConsumer<K, V> {

        private final Consumer<K, V> consumer;

        private SynchronizedConsumer(Consumer<K, V> consumer) {
            this.consumer = consumer;
        }

        synchronized ConsumerRecords<K, V> poll(long timeout) {
            synchronized (consumer) {
                return consumer.poll(timeout);
            }
        }

        synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            synchronized (consumer) {
                consumer.commitSync(offsets);
            }
        }

        synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
            synchronized (consumer) {
                consumer.commitAsync(offsets, callback);
            }
        }

        static <K, V> SynchronizedConsumer<K, V> synchronize(Consumer<K, V> consumer) {
            return new SynchronizedConsumer<>(consumer);
        }
    }

    public static class Builder {
        public static final Properties DEFAULT_KAFKA_PROPS = new Properties();

        private final Consumer<String, byte[]> kafkaConsumer;
        private Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners = new HashMap<>();
        private List<GarmadonMessageHandler> beforeInterceptHandlers = new ArrayList<>();

        static {
            DEFAULT_KAFKA_PROPS.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); //by default groupId is random
            DEFAULT_KAFKA_PROPS.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            DEFAULT_KAFKA_PROPS.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            DEFAULT_KAFKA_PROPS.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            DEFAULT_KAFKA_PROPS.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            DEFAULT_KAFKA_PROPS.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_ID);
        }

        Builder(Consumer<String, byte[]> kafkaConsumer) {
            this.kafkaConsumer = kafkaConsumer;
        }

        public static Builder stream(Consumer<String, byte[]> kafkaConsumer) {
            return new Builder(kafkaConsumer);
        }

        public Builder intercept(GarmadonMessageFilter filter, GarmadonMessageHandler handler) {
            if (this.listeners.containsKey(filter)) {
                LOGGER.warn("Multiple handlers set for filter of type {}", filter.getClass().toString());
            }

            this.listeners.put(filter, handler);
            return this;
        }

        public Builder beforeIntercept(GarmadonMessageHandler handler) {
            this.beforeInterceptHandlers.add(handler);
            return this;
        }

        public GarmadonReader build() {
            return this.build(true);
        }

        public GarmadonReader build(boolean autoSubscribe) {
            if (autoSubscribe) kafkaConsumer.subscribe(Collections.singletonList(GARMADON_TOPIC));

            return new GarmadonReader(kafkaConsumer, beforeInterceptHandlers, listeners);
        }
    }

    public interface GarmadonMessageHandler {
        void handle(GarmadonMessage msg);
    }
}
