package com.criteo.hadoop.garmadon.reader.es;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.reader.*;
import com.criteo.jvm.JVMStatisticsProtos;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A reader that pushes events to elastic search
 */
public class ElasticSearchReader implements BulkProcessor.Listener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchReader.class);

    private final boolean isPrintingToStdout = Boolean.parseBoolean(System.getProperty("garmadon.esReader.printToStdout", "false"));
    private final int bulkActions = Integer.parseInt(System.getProperty("garmadon.esReader.bulkActions", "500"));
    private final long bulkSizeMB = Long.parseLong(System.getProperty("garmadon.esReader.bulkSizeMB", "5"));
    private final long bulkFlushIntervalSec = Long.parseLong(System.getProperty("garmadon.esReader.bulkFlushIntervalSec", "10"));

    private final GarmadonReader reader;
    private final RestHighLevelClient esClient;
    private final String esIndex;
    private final BulkProcessor bulkProcessor;

    public ElasticSearchReader(Properties properties) {
        String kafkaConnectString = properties.getProperty("kafka.connection", "localhost:");
        String groupId = properties.getProperty("kafka.groupid");
        String esHost = properties.getProperty("es.host", "localhost");
        Integer esPort = Integer.parseInt(properties.getProperty("es.port", "9200"));
        String esIndex = properties.getProperty("es.index", "garmadon-");
        String esUser = properties.getProperty("es.user");
        String esPassword = properties.getProperty("es.password");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(esHost, esPort, "http")
        );

        if (esUser != null) {
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(esUser, esPassword));

            restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        }

        //setup es client
        this.esIndex = esIndex;
        esClient = new RestHighLevelClient(restClientBuilder.build());

        //setup kafka reader
        GarmadonReader.Builder builder = GarmadonReader.Builder.stream(kafkaConnectString);
        if (groupId != null) builder.withGroupId(groupId);
        if (isPrintingToStdout) builder.intercept(GarmadonMessageFilter.ANY.INSTANCE, this::printToStdout);
        reader = builder
                .intercept(GarmadonMessageFilter.ANY.INSTANCE, this::writeToES)
                .build();

        Settings settings = Settings.builder().put("node.name", "reader-garmadon").build();

        ThreadPool threadPool = new ThreadPool(settings);

        bulkProcessor = new BulkProcessor.Builder(esClient::bulkAsync, this, threadPool)
                .setBulkActions(bulkActions)
                .setBulkSize(new ByteSizeValue(bulkSizeMB, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushIntervalSec))
                .build();
    }

    public CompletableFuture<Void> startReading() {
        return reader.startReading().whenComplete((v, ex) -> {
            if (ex != null) {
                LOGGER.error("Reading was stopped due to exception");
                ex.printStackTrace();
            } else {
                LOGGER.info("Done reading !");
            }
        });
    }

    public CompletableFuture<Void> stop() {
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

    private void printToStdout(GarmadonMessage msg) {
        System.out.print(msg.getHeader().getApplicationId());
        System.out.print("|");
        System.out.print(msg.getHeader().getApplicationName());
        System.out.print("|");
        System.out.print(msg.getHeader().getContainerId());
        System.out.print("|");
        System.out.print(msg.getHeader().getHostname());
        System.out.print("|");
        System.out.print(msg.getHeader().getUserName());
        System.out.print("|");
        System.out.print(msg.getBody().toString().replaceAll("\\n", " - "));
        System.out.println();
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

        putBodySpecificFields(msg.getBody(), jsonMap);

        IndexRequest req = new IndexRequest(esIndex, "doc", "")
                .setPipeline("dailyindex")
                .source(jsonMap);

        bulkProcessor.add(req, msg.getCommittableOffset());
    }

    private void putBodySpecificFields(Object o, Map<String, Object> jsonMap) {
        if (o instanceof DataAccessEventProtos.PathEvent) {
            DataAccessEventProtos.PathEvent event = (DataAccessEventProtos.PathEvent) o;

            Date timestamp_date = new Date(event.getTimestamp());
            jsonMap.put("timestamp", timestamp_date);
            jsonMap.put("path", event.getPath());
            jsonMap.put("type", event.getType());
        } else if (o instanceof DataAccessEventProtos.FsEvent) {
            DataAccessEventProtos.FsEvent event = (DataAccessEventProtos.FsEvent) o;

            Date timestamp_date = new Date(event.getTimestamp());
            jsonMap.put("timestamp", timestamp_date);
            jsonMap.put("src_path", event.getSrcPath());
            jsonMap.put("dst_path", event.getDstPath());
            jsonMap.put("action", event.getAction());
            jsonMap.put("uri", event.getUri());
        } else if (o instanceof DataAccessEventProtos.StateEvent) {
            DataAccessEventProtos.StateEvent event = (DataAccessEventProtos.StateEvent) o;

            Date timestamp_date = new Date(event.getTimestamp());
            jsonMap.put("timestamp", timestamp_date);
            jsonMap.put("state", event.getState());
        } else if (o instanceof JVMStatisticsProtos.JVMStatisticsData) {
            JVMStatisticsProtos.JVMStatisticsData event = (JVMStatisticsProtos.JVMStatisticsData) o;

            Date timestamp_date = new Date(event.getTimestamp());
            jsonMap.put("timestamp", timestamp_date);
            for (JVMStatisticsProtos.JVMStatisticsData.Section section : event.getSectionList()) {
                for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    try {
                        jsonMap.put(section.getName() + "_" + property.getName(), Double.parseDouble(property.getValue()));
                    } catch (NumberFormatException nfe) {
                        jsonMap.put(section.getName() + "_" + property.getName(), property.getValue());
                    }
                }
            }
        } else if (o instanceof JVMStatisticsProtos.GCStatisticsData) {
            JVMStatisticsProtos.GCStatisticsData event = (JVMStatisticsProtos.GCStatisticsData) o;

            Date timestamp_date = new Date(event.getTimestamp());
            jsonMap.put("timestamp", timestamp_date);
            jsonMap.put("collector_name", event.getCollectorName());
            jsonMap.put("pause_time", event.getPauseTime());
            jsonMap.put("cause", event.getCause());
            jsonMap.put("delta_eden", event.getEdenBefore() - event.getEdenAfter());
            jsonMap.put("delta_survivor", event.getSurvivorBefore() - event.getSurvivorAfter());
            jsonMap.put("delta_old", event.getOldBefore() - event.getOldAfter());
            jsonMap.put("delta_code", event.getCodeBefore() - event.getCodeAfter());
            jsonMap.put("delta_metaspace", event.getMetaspaceBefore() - event.getMetaspaceAfter());
        } else if (o instanceof ContainerEventProtos.ContainerResourceEvent) {
            ContainerEventProtos.ContainerResourceEvent event = (ContainerEventProtos.ContainerResourceEvent) o;

            Date timestamp_date = new Date(event.getTimestamp());
            jsonMap.put("timestamp", timestamp_date);
            jsonMap.put("type", event.getType());
            jsonMap.put("value", event.getValue());
            jsonMap.put("limit", event.getLimit());
        }
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        int numberOfActions = request.numberOfActions();
        LOGGER.info("Executing Bulk[{}] with {} requests", executionId, numberOfActions);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        if (response.hasFailures()) {
            LOGGER.error("Bulk[{}] executed with failures", executionId);
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
        LOGGER.error("failed to execute Bulk[{}]", executionId);
        failure.printStackTrace();
    }

    public static void main(String[] args) throws IOException {
        // Get properties
        Properties properties = new Properties();
        try (InputStream streamPropFilePath = ElasticSearchReader.class.getResourceAsStream("/reader.properties")) {
            properties.load(streamPropFilePath);
        }

        ElasticSearchReader reader = new ElasticSearchReader(properties);

        reader.startReading().join();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> reader.stop().join()));
    }


}
