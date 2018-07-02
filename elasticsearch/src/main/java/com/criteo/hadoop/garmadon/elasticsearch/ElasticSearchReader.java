package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonMessageFilter;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.jvm.JVMStatisticsProtos;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A reader that pushes events to elastic search
 */
public class ElasticSearchReader implements BulkProcessor.Listener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchReader.class);

    private static final int CONNECTION_TIMEOUT_MS = 10000;
    private static final int NB_RETRIES = 10;

    private static final Format formatter = new SimpleDateFormat("yyyy-MM-dd");

    private final GarmadonReader reader;
    private final String esIndexPrefix;
    private final BulkProcessor bulkProcessor;


    private ElasticSearchReader(String kafkaConnectString, String kafkaGroupId, String esHost,
                                int esPort, String esIndexPrefix, String esUser, String esPassword) {
        int bulkConcurrent = Integer.getInteger("garmadon.esReader.bulkConcurrent", 10);
        int bulkActions = Integer.getInteger("garmadon.esReader.bulkActions", 500);
        int bulkSizeMB = Integer.getInteger("garmadon.esReader.bulkSizeMB", 5);
        int bulkFlushIntervalSec = Integer.getInteger("garmadon.esReader.bulkFlushIntervalSec", 10);

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(esHost, esPort, "http")
        );

        if (esUser != null) {
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(esUser, esPassword));

            restClientBuilder
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider))
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                            .setConnectTimeout(CONNECTION_TIMEOUT_MS)
                            .setConnectionRequestTimeout(CONNECTION_TIMEOUT_MS)
                            //.setSocketTimeout(10000)
                            .setContentCompressionEnabled(true));
        }

        //setup es client
        this.esIndexPrefix = esIndexPrefix;
        RestHighLevelClient esClient = new RestHighLevelClient(restClientBuilder);

        //setup kafka reader
        GarmadonReader.Builder builder = GarmadonReader.Builder.stream(kafkaConnectString);
        reader = builder
                .withGroupId(kafkaGroupId)
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
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }

    private void writeToES(GarmadonMessage msg) {
        Map<String, Object> jsonMap = new HashMap<>();

        if (msg.getHeader().hasApplicationId())
            jsonMap.put("application_id", msg.getHeader().getApplicationId());
        if (msg.getHeader().hasAppAttemptID())
            jsonMap.put("attempt_id", msg.getHeader().getAppAttemptID());
        if (msg.getHeader().hasApplicationName())
            jsonMap.put("application_name", msg.getHeader().getApplicationName());
        if (msg.getHeader().hasContainerId())
            jsonMap.put("container_id", msg.getHeader().getContainerId());
        if (msg.getHeader().hasHostname())
            jsonMap.put("hostname", msg.getHeader().getHostname());
        if (msg.getHeader().hasUserName())
            jsonMap.put("username", msg.getHeader().getUserName());
        if (msg.getHeader().hasTag())
            jsonMap.put("tag", msg.getHeader().getTag());
        if (msg.getHeader().hasPid())
            jsonMap.put("pid", msg.getHeader().getPid());
        if (msg.getHeader().hasFramework())
            jsonMap.put("framework", msg.getHeader().getFramework());
        if (msg.getHeader().hasComponent())
            jsonMap.put("component", msg.getHeader().getComponent());

        HashMap<String, Map<String, Object>> eventMaps = putBodySpecificFields(msg.getBody());
        for (Map<String, Object> eventMap : eventMaps.values()) {
            eventMap.putAll(jsonMap);
            String daily_index = esIndexPrefix + "-" + formatter.format(eventMap.get("timestamp"));
            IndexRequest req = new IndexRequest(daily_index, "doc")
                    .source(eventMap);
            bulkProcessor.add(req, msg.getCommittableOffset());
        }
    }

    private HashMap<String, Map<String, Object>> putBodySpecificFields(Object o) {
        HashMap<String, Map<String, Object>> eventMaps = new HashMap<>();

        if (o instanceof DataAccessEventProtos.FsEvent) {
            EventHelper.processFsEvent((DataAccessEventProtos.FsEvent) o, eventMaps);
        } else if (o instanceof DataAccessEventProtos.StateEvent) {
            EventHelper.processStateEvent((DataAccessEventProtos.StateEvent) o, eventMaps);
        } else if (o instanceof JVMStatisticsProtos.JVMStatisticsData) {
            EventHelper.processJVMStatisticsData((JVMStatisticsProtos.JVMStatisticsData) o, eventMaps);
        } else if (o instanceof JVMStatisticsProtos.GCStatisticsData) {
            EventHelper.processGCStatisticsData((JVMStatisticsProtos.GCStatisticsData) o, eventMaps);
        } else if (o instanceof ContainerEventProtos.ContainerResourceEvent) {
            EventHelper.processContainerResourceEvent((ContainerEventProtos.ContainerResourceEvent) o, eventMaps);
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
                    LOGGER.error("Bulk failed on {} due to {}", item.getId(), item.getFailureMessage());
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
                    } else {
                        LOGGER.info("Committed kafka offset {}|{}", topicPartitionOffset.getOffset(), topicPartitionOffset.getPartition());
                    }
                });
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        LOGGER.error("Failed to execute Bulk[{}]", executionId, failure);
    }

    public static void main(String[] args) {

        if (args.length < 7) {
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


        ElasticSearchReader reader = new ElasticSearchReader(kafkaConnectString, kafkaGroupId, esHost,
                esPort, esIndexPrefix, esUser, esPassword);

        reader.startReading().join();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> reader.stop().join()));
    }


    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\tjava com.criteo.hadoop.garmadon.elasticsearch.ElasticSearchReader <kafkaConnectionString> <kafkaGroupId> <EsHost> <EsPort> <esIndexPrefix> <EsUser> <EsPassword>");
    }
}
