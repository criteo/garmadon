package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.event.proto.*;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonMessageFilter;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import io.prometheus.client.Counter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A reader that pushes events to elastic search
 */
public final class ElasticSearchReader implements BulkProcessor.Listener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchReader.class);

    private static Counter.Child numberOfEventInError = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS.labels("number_of_event_in_error",
            GarmadonReader.getHostname(),
            PrometheusHttpConsumerMetrics.RELEASE);
    private static Counter.Child numberOfOffsetCommitError = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS.labels("number_of_offset_commit_error",
            GarmadonReader.getHostname(),
            PrometheusHttpConsumerMetrics.RELEASE);

    private static final int CONNECTION_TIMEOUT_MS = 10000;
    private static final int SOCKET_TIMEOUT_MS = 60000;
    private static final int NB_RETRIES = 10;

    private static final Format FORMATTER = new SimpleDateFormat("yyyy-MM-dd-HH");

    private final GarmadonReader reader;
    private final String esIndexPrefix;
    private final BulkProcessor bulkProcessor;
    private PrometheusHttpConsumerMetrics prometheusHttpConsumerMetrics;


    private ElasticSearchReader(String kafkaConnectString, String kafkaGroupId, String esHost,
                                int esPort, String esIndexPrefix, String esUser, String esPassword, int prometheusPort) {
        int bulkConcurrent = Integer.getInteger("garmadon.esReader.bulkConcurrent", 10);
        int bulkActions = Integer.getInteger("garmadon.esReader.bulkActions", 500);
        int bulkSizeMB = Integer.getInteger("garmadon.esReader.bulkSizeMB", 5);
        int bulkFlushIntervalSec = Integer.getInteger("garmadon.esReader.bulkFlushIntervalSec", 10);

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(esHost, esPort, "http")
        )
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
        this.esIndexPrefix = esIndexPrefix;
        RestHighLevelClient esClient = new RestHighLevelClient(restClientBuilder);

        //setup Prometheus client
        prometheusHttpConsumerMetrics = new PrometheusHttpConsumerMetrics(prometheusPort);

        //setup kafka reader
        Properties props = new Properties();

        props.putAll(GarmadonReader.Builder.DEFAULT_KAFKA_PROPS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectString);
        GarmadonReader.Builder builder = GarmadonReader.Builder.stream(new KafkaConsumer<>(props));
        reader = builder
                .intercept(GarmadonMessageFilter.ANY.INSTANCE, this::writeToES)
                .build();

        bulkProcessor = BulkProcessor.builder(esClient::bulkAsync, this)
                .setBulkActions(bulkActions)
                .setBulkSize(new ByteSizeValue(bulkSizeMB, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushIntervalSec))
                .setConcurrentRequests(bulkConcurrent)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), NB_RETRIES)
                )
                .build();
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

    private void writeToES(GarmadonMessage msg) {
        Map<String, Object> jsonMap = new HashMap<>();

        jsonMap.put("timestamp", msg.getTimestamp());
        jsonMap.put("application_id", msg.getHeader().getApplicationId());
        jsonMap.put("attempt_id", msg.getHeader().getAppAttemptId());
        jsonMap.put("application_name", msg.getHeader().getApplicationName());
        jsonMap.put("container_id", msg.getHeader().getContainerId());
        jsonMap.put("hostname", msg.getHeader().getHostname());
        jsonMap.put("username", msg.getHeader().getUserName());
        jsonMap.put("pid", msg.getHeader().getPid());
        jsonMap.put("framework", msg.getHeader().getFramework());
        jsonMap.put("component", msg.getHeader().getComponent());
        jsonMap.put("executor_id", msg.getHeader().getExecutorId());
        jsonMap.put("main_class", msg.getHeader().getMainClass());
        jsonMap.put("tags", msg.getHeader().getTagsList());

        HashMap<String, Map<String, Object>> eventMaps = putBodySpecificFields(
                GarmadonSerialization.getTypeName(msg.getType()), msg.getBody());
        for (Map<String, Object> eventMap : eventMaps.values()) {
            eventMap.putAll(jsonMap);
            String dailyIndex = esIndexPrefix + "-" + FORMATTER.format(msg.getTimestamp());
            IndexRequest req = new IndexRequest(dailyIndex, "doc")
                    .source(eventMap);
            bulkProcessor.add(req, msg.getCommittableOffset());
        }
    }

    private HashMap<String, Map<String, Object>> putBodySpecificFields(String type, Object o) {
        HashMap<String, Map<String, Object>> eventMaps = new HashMap<>();

        if (o instanceof DataAccessEventProtos.FsEvent) {
            EventHelper.processFsEvent(type, (DataAccessEventProtos.FsEvent) o, eventMaps);
        } else if (o instanceof DataAccessEventProtos.StateEvent) {
            EventHelper.processStateEvent(type, (DataAccessEventProtos.StateEvent) o, eventMaps);
        } else if (o instanceof SparkEventProtos.StageEvent) {
            EventHelper.processStageEvent(type, (SparkEventProtos.StageEvent) o, eventMaps);
        } else if (o instanceof SparkEventProtos.StageStateEvent) {
            EventHelper.processStageStateEvent(type, (SparkEventProtos.StageStateEvent) o, eventMaps);
        } else if (o instanceof SparkEventProtos.ExecutorStateEvent) {
            EventHelper.processExecutorStateEvent(type, (SparkEventProtos.ExecutorStateEvent) o, eventMaps);
        } else if (o instanceof SparkEventProtos.TaskEvent) {
            EventHelper.processTaskEvent(type, (SparkEventProtos.TaskEvent) o, eventMaps);
        } else if (o instanceof JVMStatisticsEventsProtos.JVMStatisticsData) {
            EventHelper.processJVMStatisticsData(type, (JVMStatisticsEventsProtos.JVMStatisticsData) o, eventMaps);
        } else if (o instanceof ContainerEventProtos.ContainerResourceEvent) {
            EventHelper.processContainerResourceEvent(type, (ContainerEventProtos.ContainerResourceEvent) o, eventMaps);
        } else if (o instanceof ResourceManagerEventProtos.ApplicationEvent) {
            EventHelper.processApplicationEvent(type, (ResourceManagerEventProtos.ApplicationEvent) o, eventMaps);
        }
        return eventMaps;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        LOGGER.debug("Executing Bulk[{}] with {} requests of {} Bytes", executionId,
                request.numberOfActions(),
                request.estimatedSizeInBytes());
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        if (response.hasFailures()) {
            LOGGER.error("Bulk[{}] executed with failures", executionId);
            for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed()) {
                    LOGGER.error("Failed on {} due to {}", item.getId(), item.getFailureMessage());
                    numberOfEventInError.inc();
                }
            }
        } else {
            LOGGER.info("Successfully completed Bulk[{}] in {} ms", executionId, response.getTook().getMillis());
        }
        CommittableOffset<String, byte[]> lastOffset = ((CommittableOffset<String, byte[]>) request.payloads().get(request.payloads().size() - 1));
        lastOffset
                .commitAsync()
                .whenComplete((topicPartitionOffset, exception) -> {
                    if (exception != null) {
                        LOGGER.warn("Could not commit kafka offset {}|{}", lastOffset.getOffset(), lastOffset.getPartition());
                        numberOfOffsetCommitError.inc();
                    } else {
                        LOGGER.info("Committed kafka offset {}|{}", topicPartitionOffset.getOffset(), topicPartitionOffset.getPartition());
                    }
                });
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        LOGGER.error("Failed to execute Bulk[{}]", executionId, failure);
        numberOfEventInError.inc(request.requests().size());
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

        ElasticSearchReader reader = new ElasticSearchReader(kafkaConnectString, kafkaGroupId, esHost,
                esPort, esIndexPrefix, esUser, esPassword, prometheusPort);

        reader.startReading().join();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> reader.stop().join()));
    }


    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\tjava com.criteo.hadoop.garmadon.elasticsearch.ElasticSearchReader <kafkaConnectionString> <kafkaGroupId> <EsHost> <EsPort> <esIndexPrefix> <EsUser> <EsPassword> <prometheusPort>");
    }
}
