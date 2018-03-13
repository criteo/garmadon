package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.schema.exceptions.DeserializationException;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.sun.corba.se.impl.orbutil.concurrent.Sync;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class GarmadonReader {

    private final Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners;
    private final KafkaConsumer<String, byte[]> kafkaConsumer;
    private Reader reader;
    private CompletableFuture<Void> cf;
    private boolean reading = false;

    private GarmadonReader(String groupId, String kafkaConnectString, Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners) {
        this.listeners = listeners;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectString);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.kafkaConsumer.subscribe(Collections.singletonList("GARMADON"));
    }

    /**
     * returns a future that completes when consuming is done
     */
    public synchronized CompletableFuture<Void> startReading() {
        if (!reading) {
            cf = new CompletableFuture<>();
            reader = new Reader(kafkaConsumer, listeners, cf);
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
        private final Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners;
        private final Set<GarmadonMessageFilter> filters;

        private volatile boolean keepOnReading = true;

        Reader(KafkaConsumer<String, byte[]> consumer, Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners, CompletableFuture<Void> cf) {
            this.consumer = SynchronizedConsumer.synchronize(consumer);
            this.listeners = listeners;
            this.filters = listeners.keySet();
            this.cf = cf;
        }

        @Override
        public void run() {
            try {
                while (keepOnReading) {

                    ConsumerRecords<String, byte[]> records = consumer.poll(1000L);

                    for (ConsumerRecord<String, byte[]> record : records) {
                        byte[] raw = record.value();
                        ByteBuffer buf = ByteBuffer.wrap(raw);

                        int typeMarker = buf.getInt();
                        int headerSize = buf.getInt();
                        int bodySize = buf.getInt();

                        DataAccessEventProtos.Header header = null;
                        Object body = null;

                        for (GarmadonMessageFilter filter : filters) {
                            if (filter.acceptsType(typeMarker)) {

                                if (header == null) {
                                    header = DataAccessEventProtos.Header.parseFrom(new ByteArrayInputStream(raw, HEADER_OFFSET, headerSize));
                                }

                                if (filter.acceptsHeader(header)) {

                                    if (body == null) {
                                        int bodyOffset = HEADER_OFFSET + headerSize;
                                        try {
                                            body = GarmadonSerialization.parseFrom(typeMarker, new ByteArrayInputStream(raw, bodyOffset, bodySize));
                                        } catch (DeserializationException e) {
                                            System.err.println("cannot deserialize event from kafka record " + record + "with type " + typeMarker);
                                        }
                                    }

                                    CommittableOffset<String, byte[]> committableOffset = new CommittableOffset<>(consumer, record.topic(), record.partition(), record.offset());
                                    GarmadonMessage msg = new GarmadonMessage(typeMarker, header, body, committableOffset);
                                    listeners.get(filter).handle(msg);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                cf.completeExceptionally(e);
            }
            cf.complete(null);
        }

        void stop() {
            keepOnReading = false;
        }
    }

    public static class SynchronizedConsumer<K, V> {

        private final KafkaConsumer<K, V> consumer;

        private SynchronizedConsumer(KafkaConsumer<K, V> consumer) {
            this.consumer = consumer;
        }

        public synchronized ConsumerRecords<K, V> poll(long timeout) {
            return consumer.poll(timeout);
        }

        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            consumer.commitSync(offsets);
        }

        public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback){
            consumer.commitAsync(offsets, callback);
        }

        public static <K,V> SynchronizedConsumer<K, V> synchronize(KafkaConsumer<K,V> consumer){
            return new SynchronizedConsumer<>(consumer);
        }
    }

    public static class Builder {

        private String groupId = UUID.randomUUID().toString(); //by default groupId is random
        private String kafkaConnectString;
        private Map<GarmadonMessageFilter, GarmadonMessageHandler> listeners = new HashMap<>();

        Builder(String kafkaConnectString) {
            this.kafkaConnectString = kafkaConnectString;
        }

        public static Builder withKafkaConnectString(String kafkaConnectString) {
            return new Builder(kafkaConnectString);
        }

        public Builder withGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder listeningToMessages(GarmadonMessageFilter filter, GarmadonMessageHandler handler) {
            this.listeners.put(filter, handler);
            return this;
        }

        public GarmadonReader build() {
            return new GarmadonReader(groupId, kafkaConnectString, listeners);
        }
    }

    public interface GarmadonMessageHandler {
        void handle(GarmadonMessage msg);
    }
}
