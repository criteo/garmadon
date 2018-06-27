package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.exceptions.DeserializationException;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class GarmadonReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(GarmadonReader.class);

    private final Reader reader;
    private final CompletableFuture<Void> cf;

    private boolean reading = false;

    private GarmadonReader(List<GarmadonMessageHandler> beforeInterceptHandlers, Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners, Properties props) {
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList("garmadon"));

        this.cf = new CompletableFuture<>();
        this.reader = new Reader(kafkaConsumer, beforeInterceptHandlers, listeners, cf);
    }

    /**
     * returns a future that completes when consuming is done
     */
    public synchronized CompletableFuture<Void> startReading() {
        if (!reading) {
            new Thread(reader).start();
            reading = true;
        }
        return cf;
    }

    /**
     * returns a future that completes when consuming is done
     */
    public synchronized CompletableFuture<Void> stopReading() {
        if (reading) {
            reader.stop();
            return cf;
        } else return CompletableFuture.completedFuture(null);
    }

    private static class Reader implements Runnable {

        private static final int HEADER_OFFSET = 12;

        private final SynchronizedConsumer<String, byte[]> consumer;
        private final CompletableFuture<Void> cf;
        private final List<GarmadonMessageHandler> beforeInterceptHandlers;
        private final Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners;
        private final Set<GarmadonMessageFilter> filters;

        private final Counter receivedCounter = new Counter();

        private volatile boolean keepOnReading = true;

        Reader(KafkaConsumer<String, byte[]> consumer, List<GarmadonMessageHandler> beforeInterceptHandlers, Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners, CompletableFuture<Void> cf) {
            this.consumer = SynchronizedConsumer.synchronize(consumer);
            this.beforeInterceptHandlers = beforeInterceptHandlers;
            this.listeners = listeners;
            this.filters = listeners.keySet();
            this.cf = cf;
        }

        @Override
        public void run() {
            try {

                LOGGER.info("start reading");

                while (keepOnReading) {

                    ConsumerRecords<String, byte[]> records = consumer.poll(1000L);

                    for (ConsumerRecord<String, byte[]> record : records) {

                        receivedCounter.increment();

                        byte[] raw = record.value();
                        ByteBuffer buf = ByteBuffer.wrap(raw);

                        int typeMarker = buf.getInt();
                        int headerSize = buf.getInt();
                        int bodySize = buf.getInt();

                        DataAccessEventProtos.Header header = null;
                        Object body = null;

                        for (GarmadonMessageFilter filter : filters) {
                            if (filter.accepts(typeMarker)) {

                                if (header == null) {
                                    header = DataAccessEventProtos.Header.parseFrom(new ByteArrayInputStream(raw, HEADER_OFFSET, headerSize));
                                }

                                if (filter.accepts(typeMarker, header)) {

                                    if (body == null) {
                                        int bodyOffset = HEADER_OFFSET + headerSize;
                                        try {
                                            body = GarmadonSerialization.parseFrom(typeMarker, new ByteArrayInputStream(raw, bodyOffset, bodySize));
                                        } catch (DeserializationException e) {
                                            LOGGER.error("Cannot deserialize event from kafka record " + record + "with type " + typeMarker);
                                        }
                                    }

                                    CommittableOffset<String, byte[]> committableOffset = new CommittableOffset<>(consumer, record.topic(), record.partition(), record.offset());
                                    GarmadonMessage msg = new GarmadonMessage(typeMarker, header, body, committableOffset);

                                    beforeInterceptHandlers.forEach(c -> c.handle(msg));
                                    listeners.get(filter).handle(msg);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("unexpected exception while reading", e);
                cf.completeExceptionally(e);
            }

            LOGGER.info("stopped reading");
            cf.complete(null);

            LOGGER.info("received {} messages", receivedCounter.get());
        }

        void stop() {
            keepOnReading = false;
        }

        private static class Counter {

            int count = 0;

            void increment(){
                count++;
                if(count % 500 == 0){
                    LOGGER.debug("Received {} messages so far", count);
                }
            }

            int get(){
                return count;
            }
        }
    }

    static class SynchronizedConsumer<K, V> {

        private final KafkaConsumer<K, V> consumer;

        private SynchronizedConsumer(KafkaConsumer<K, V> consumer) {
            this.consumer = consumer;
        }

        synchronized ConsumerRecords<K, V> poll(long timeout) {
            return consumer.poll(timeout);
        }

        synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            consumer.commitSync(offsets);
        }

        synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback){
            consumer.commitAsync(offsets, callback);
        }

        static <K,V> SynchronizedConsumer<K, V> synchronize(KafkaConsumer<K, V> consumer){
            return new SynchronizedConsumer<>(consumer);
        }
    }

    public static class Builder {

        private Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners = new HashMap<>();
        private List<GarmadonMessageHandler> beforeInterceptHandlers = new ArrayList<>();
        private Properties props = new Properties();

        Builder(String kafkaConnectString) {
            this.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectString);
            this.props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); //by default groupId is random
            this.props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            this.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            this.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        }

        public static Builder stream(String kafkaConnectString) {
            return new Builder(kafkaConnectString);
        }

        public Builder withGroupId(String groupId) {
            this.props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            return this;
        }

        public Builder withKafkaProp(String key, Object value) {
            this.props.put(key, value);
            return this;
        }

        public Builder intercept(GarmadonMessageFilter filter, GarmadonMessageHandler handler) {
            this.listeners.put(filter, handler);
            return this;
        }

        public Builder beforeIntercept(GarmadonMessageHandler handler) {
            this.beforeInterceptHandlers.add(handler);
            return this;
        }

        public GarmadonReader build() {
            return new GarmadonReader(beforeInterceptHandlers, listeners, props);
        }
    }

    public interface GarmadonMessageHandler {
        void handle(GarmadonMessage msg);
    }
}
