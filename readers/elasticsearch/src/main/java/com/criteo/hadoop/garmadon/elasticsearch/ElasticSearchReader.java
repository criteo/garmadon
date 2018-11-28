package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.criteo.hadoop.garmadon.protobuf.ProtoConcatenator;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.hasType;
import static com.criteo.hadoop.garmadon.reader.GarmadonMessageFilters.not;

/**
 * A reader that pushes events to elastic search
 */
public final class ElasticSearchReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchReader.class);

    private static final int CONNECTION_TIMEOUT_MS = 10000;
    private static final int SOCKET_TIMEOUT_MS = 60000;
    private static final int NB_RETRIES = 10;

    private static final Format FORMATTER = new SimpleDateFormat("yyyy-MM-dd-HH");

    private final GarmadonReader reader;
    private final String esIndexPrefix;
    private final BulkProcessor bulkProcessor;
    private PrometheusHttpConsumerMetrics prometheusHttpConsumerMetrics;


    ElasticSearchReader(GarmadonReader.Builder builderReader,
                        BulkProcessor bulkProcessorMain,
                        String esIndexPrefix,
                        PrometheusHttpConsumerMetrics prometheusHttpConsumerMetrics) {
        this.reader = builderReader
                .intercept(not(hasType(GarmadonSerialization.TypeMarker.GC_EVENT)), this::writeToES)
                .build();

        this.bulkProcessor = bulkProcessorMain;

        this.esIndexPrefix = esIndexPrefix;
        this.prometheusHttpConsumerMetrics = prometheusHttpConsumerMetrics;
    }

    private CompletableFuture<Void> startReading() {
        return reader.startReading().whenComplete((v, ex) -> {
            if (ex != null) {
                LOGGER.error("Reading was stopped due to exception");
                ex.printStackTrace();
            } else {
                LOGGER.info("Done reading !");
            }
        });
    }

    private CompletableFuture<Void> stop() {
        return reader
                .stopReading()
                .whenComplete((vd, ex) -> {
                    try {
                        bulkProcessor.awaitClose(10L, TimeUnit.SECONDS);
                        prometheusHttpConsumerMetrics.terminate();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }

    void writeToES(GarmadonMessage msg) {
        String msgType = GarmadonSerialization.getTypeName(msg.getType());
        if (msgType.equals("JVMSTATS_EVENT")) {
            Map<String, Object> jsonMap = ProtoConcatenator.concatToMap(Arrays.asList(msg.getHeader()), true);

            HashMap<String, Map<String, Object>> eventMaps = new HashMap<>();
            EventHelper.processJVMStatisticsData(msgType, (JVMStatisticsEventsProtos.JVMStatisticsData) msg.getBody(), eventMaps);

            for (Map<String, Object> eventMap : eventMaps.values()) {
                eventMap.putAll(jsonMap);
                addEventToBulkProcessor(eventMap, msg.getTimestamp(), msg.getCommittableOffset());
            }
        } else {
            Map<String, Object> eventMap = ProtoConcatenator.concatToMap(Arrays.asList(msg.getHeader(), msg.getBody()), true);
            eventMap.put("event_type", msgType);
            addEventToBulkProcessor(eventMap, msg.getTimestamp(), msg.getCommittableOffset());
        }
    }

    private void addEventToBulkProcessor(Map<String, Object> eventMap, long timestamp, CommittableOffset committableOffset) {
        eventMap.put("timestamp", timestamp);
        eventMap.remove("id"); // field only used as kafka key

        String dailyIndex = esIndexPrefix + "-" + FORMATTER.format(timestamp);
        IndexRequest req = new IndexRequest(dailyIndex, "doc")
                .source(eventMap);
        bulkProcessor.add(req, committableOffset);
    }

    private static class LogFailureListener extends SniffOnFailureListener {
        LogFailureListener() {
            super();
        }

        @Override
        public void onFailure(HttpHost host) {
            LOGGER.warn("Node failed: " + host.getHostName() + "-" + host.getPort());
            super.onFailure(host);
        }
    }

    private static GarmadonReader.Builder setUpKafkaReader(String kafkaConnectString, String kafkaGroupId) {

        //setup kafka reader
        Properties props = new Properties();

        props.putAll(GarmadonReader.Builder.DEFAULT_KAFKA_PROPS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectString);
        return GarmadonReader.Builder.stream(new KafkaConsumer<>(props));
    }

    private static BulkProcessor setUpBulkProcessor(String esHost, int esPort, String esUser, String esPassword) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        int bulkConcurrent = Integer.getInteger("garmadon.esReader.bulkConcurrent", 10);
        int bulkActions = Integer.getInteger("garmadon.esReader.bulkActions", 500);
        int bulkSizeMB = Integer.getInteger("garmadon.esReader.bulkSizeMB", 5);
        int bulkFlushIntervalSec = Integer.getInteger("garmadon.esReader.bulkFlushIntervalSec", 10);

        LogFailureListener sniffOnFailureListener = new LogFailureListener();
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(esHost, esPort, "http")
        )
                .setFailureListener(sniffOnFailureListener)
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(CONNECTION_TIMEOUT_MS)
                        .setSocketTimeout(SOCKET_TIMEOUT_MS)
                        .setContentCompressionEnabled(true))
                .setMaxRetryTimeoutMillis(2 * SOCKET_TIMEOUT_MS);

        if (esUser != null) {
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(esUser, esPassword));

            restClientBuilder
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider));
        }

        //setup es client
        RestHighLevelClient esClient = new RestHighLevelClient(restClientBuilder);

        Sniffer sniffer = Sniffer.builder(esClient.getLowLevelClient()).build();
        sniffOnFailureListener.setSniffer(sniffer);

        return BulkProcessor.builder(esClient::bulkAsync, new ElasticSearchListener())
                .setBulkActions(bulkActions)
                .setBulkSize(new ByteSizeValue(bulkSizeMB, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushIntervalSec))
                .setConcurrentRequests(bulkConcurrent)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), NB_RETRIES)
                )
                .build();

    }

    public static void main(String[] args) throws IOException {
        if (args.length < 8) {
            printHelp();
            return;
        }
        String kafkaConnectString = args[0];
        String kafkaGroupId = args[1];
        String esHost = args[2];
        int esPort = Integer.parseInt(args[3]);
        String esIndexPrefix = args[4];
        String esUser = args[5];
        String esPassword = args[6];
        int prometheusPort = Integer.parseInt(args[7]);

        GarmadonReader.Builder builderReader = setUpKafkaReader(kafkaConnectString, kafkaGroupId);
        BulkProcessor bulkProcessorMain = setUpBulkProcessor(esHost, esPort, esUser, esPassword);

        ElasticSearchReader reader = new ElasticSearchReader(builderReader, bulkProcessorMain,
                esIndexPrefix, new PrometheusHttpConsumerMetrics(prometheusPort));

        reader.startReading().join();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> reader.stop().join()));
    }


    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\tjava com.criteo.hadoop.garmadon.elasticsearch.ElasticSearchReader <kafkaConnectionString> " +
                "<kafkaGroupId> <EsHost> <EsPort> <esIndexPrefix> <EsUser> <EsPassword> <prometheusPort>");
    }
}
