package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.elasticsearch.configurations.ElasticsearchConfiguration;
import com.criteo.hadoop.garmadon.elasticsearch.configurations.EsReaderConfiguration;
import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonMessageFilter;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.configurations.KafkaConfiguration;
import com.criteo.hadoop.garmadon.reader.configurations.ReaderConfiguration;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * A reader that pushes events to elastic search
 */
public final class ElasticSearchReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchReader.class);

    private static final int CONNECTION_TIMEOUT_MS = 10000;
    private static final int SOCKET_TIMEOUT_MS = 60000;
    private static final int NB_RETRIES = 10;

    private static final Format FORMATTER = new SimpleDateFormat("yyyy-MM-dd-HH");

    private static final String ES_TYPE = "doc";

    private final GarmadonReader reader;
    private final String esIndexPrefix;
    private final BulkProcessor bulkProcessor;
    private PrometheusHttpConsumerMetrics prometheusHttpConsumerMetrics;


    ElasticSearchReader(GarmadonReader.Builder builderReader,
                        BulkProcessor bulkProcessorMain,
                        String esIndexPrefix,
                        PrometheusHttpConsumerMetrics prometheusHttpConsumerMetrics) {
        this.reader = builderReader
            .intercept(GarmadonMessageFilter.ANY.INSTANCE, this::writeToES)
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
        long timestampMillis = msg.getTimestamp();
        if (GarmadonSerialization.TypeMarker.JVMSTATS_EVENT == msg.getType()) {
            Map<String, Object> jsonMap = msg.getHeaderMap(true);

            HashMap<String, Map<String, Object>> eventMaps = new HashMap<>();
            EventHelper.processJVMStatisticsData(msgType, (JVMStatisticsEventsProtos.JVMStatisticsData) msg.getBody(), eventMaps);

            for (Map<String, Object> eventMap : eventMaps.values()) {
                eventMap.putAll(jsonMap);
                addEventToBulkProcessor(eventMap, timestampMillis, msg.getCommittableOffset());
            }
        } else {
            Map<String, Object> eventMap = msg.toMap(true, true);

            addEventToBulkProcessor(eventMap, timestampMillis, msg.getCommittableOffset());
        }
    }

    private void addEventToBulkProcessor(Map<String, Object> eventMap, long timestampMillis, CommittableOffset committableOffset) {
        eventMap.remove("id"); // field only used as kafka key

        String dailyIndex = esIndexPrefix + "-" + FORMATTER.format(timestampMillis);
        IndexRequest req = new IndexRequest(dailyIndex, ES_TYPE)
            .source(eventMap);
        bulkProcessor.add(req, committableOffset);
    }

    private static class LogFailureListener extends SniffOnFailureListener {
        LogFailureListener() {
            super();
        }

        @Override
        public void onFailure(Node node) {
            LOGGER.warn("Node failed: " + node.getHost().getHostName() + "-" + node.getHost().getPort());
            super.onFailure(node);
        }
    }

    private static GarmadonReader.Builder setUpKafkaReader(KafkaConfiguration kafka) {
        //setup kafka reader
        Properties props = new Properties();

        props.putAll(GarmadonReader.Builder.DEFAULT_KAFKA_PROPS);
        props.putAll(kafka.getSettings());

        return GarmadonReader.Builder.stream(new KafkaConsumer<>(props));
    }

    private static void putGarmadonTemplate(RestHighLevelClient esClient, ElasticsearchConfiguration elasticsearch)
        throws IOException, GarmadonEsException {
        PutIndexTemplateRequest indexRequest = new PutIndexTemplateRequest("garmadon");
        indexRequest.patterns(Collections.singletonList(elasticsearch.getIndexPrefix() + "*"));

        // Create template settings with mandatory one
        Settings.Builder templateSettings = Settings.builder()
            .put("sort.field", "timestamp")
            .put("sort.order", "desc")
            .put("analysis.analyzer.path_analyzer.tokenizer", "path_tokenizer")
            .put("analysis.tokenizer.path_tokenizer.type", "path_hierarchy")
            .put("analysis.tokenizer.path_tokenizer.delimiter", "/");

        // Add settings from config
        elasticsearch.getSettings().forEach((key, value) -> templateSettings.put(key, value));

        indexRequest.settings(templateSettings);

        String template = IOUtils.toString(Objects.requireNonNull(ElasticSearchReader.class.getClassLoader()
            .getResourceAsStream("template.json")), "UTF-8");

        indexRequest.mapping(ES_TYPE, template, XContentType.JSON);

        if (!esClient.indices().putTemplate(indexRequest, RequestOptions.DEFAULT).isAcknowledged()) {
            throw new GarmadonEsException("Failed to insert garmadon template");
        }
    }

    private static BulkProcessor setUpBulkProcessor(ElasticsearchConfiguration elasticsearch) throws IOException, GarmadonEsException {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        LogFailureListener sniffOnFailureListener = new LogFailureListener();
        RestClientBuilder restClientBuilder = RestClient.builder(
            new HttpHost(elasticsearch.getHost(), elasticsearch.getPort(), "http")
        )
            .setFailureListener(sniffOnFailureListener)
            .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                .setConnectTimeout(CONNECTION_TIMEOUT_MS)
                .setSocketTimeout(SOCKET_TIMEOUT_MS)
                .setContentCompressionEnabled(true))
            .setMaxRetryTimeoutMillis(2 * SOCKET_TIMEOUT_MS);

        if (elasticsearch.getUser() != null) {
            credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(elasticsearch.getUser(), elasticsearch.getPassword()));

            restClientBuilder
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                    .setDefaultCredentialsProvider(credentialsProvider));
        }

        //setup es client
        RestHighLevelClient esClient = new RestHighLevelClient(restClientBuilder);

        putGarmadonTemplate(esClient, elasticsearch);

        Sniffer sniffer = Sniffer.builder(esClient.getLowLevelClient()).build();
        sniffOnFailureListener.setSniffer(sniffer);

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
            (request, bulkListener) -> esClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        return BulkProcessor.builder(bulkConsumer, new ElasticSearchListener())
            .setBulkActions(elasticsearch.getBulkActions())
            .setBulkSize(new ByteSizeValue(elasticsearch.getBulkSizeMB(), ByteSizeUnit.MB))
            .setFlushInterval(TimeValue.timeValueSeconds(elasticsearch.getBulkFlushIntervalSec()))
            .setConcurrentRequests(elasticsearch.getBulkConcurrent())
            .setBackoffPolicy(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), NB_RETRIES)
            )
            .build();
    }

    public static void main(String[] args) throws IOException, GarmadonEsException {
        EsReaderConfiguration config = ReaderConfiguration.loadConfig(EsReaderConfiguration.class);

        GarmadonReader.Builder builderReader = setUpKafkaReader(config.getKafka());
        BulkProcessor bulkProcessorMain = setUpBulkProcessor(config.getElasticsearch());

        ElasticSearchReader reader = new ElasticSearchReader(builderReader, bulkProcessorMain,
            config.getElasticsearch().getIndexPrefix(), new PrometheusHttpConsumerMetrics(config.getPrometheus().getPort()));

        reader.startReading().join();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> reader.stop().join()));
    }
}
