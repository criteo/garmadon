package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.elasticsearch.cache.ElasticSearchCacheManager;
import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonMessageFilter;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import com.criteo.hadoop.garmadon.schema.enums.FsAction;
import com.criteo.hadoop.garmadon.schema.enums.State;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ElasticSearchReaderTest {
    private GarmadonReader.Builder garmadonReaderBuilder;
    private BulkProcessor bulkProcessor;
    private PrometheusHttpConsumerMetrics prometheusHttpConsumerMetrics;
    private ElasticSearchCacheManager elasticSearchCacheManager;
    private static EventHeaderProtos.Header header;
    Map<String, Object> headerMap = new HashMap<>();
    private static ElasticSearchReader elasticSearchReader;

    ArgumentCaptor<IndexRequest> argument = ArgumentCaptor.forClass(IndexRequest.class);

    @Before
    public void setUp() {
        GarmadonReader.GarmadonMessageHandler garmadonMessageHandler = mock(GarmadonReader.GarmadonMessageHandler.class);
        MockConsumer kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        GarmadonReader.Builder builder = GarmadonReader.Builder.stream(kafkaConsumer);
        GarmadonReader garmadonReader = builder
                .intercept(GarmadonMessageFilter.ANY.INSTANCE, garmadonMessageHandler)
                .build(false);

        garmadonReaderBuilder = Mockito.mock(GarmadonReader.Builder.class);
        when(garmadonReaderBuilder.intercept(any(GarmadonMessageFilter.class), any(GarmadonReader.GarmadonMessageHandler.class))).thenReturn(garmadonReaderBuilder);
        when(garmadonReaderBuilder.build()).thenReturn(garmadonReader);

        bulkProcessor = Mockito.mock(BulkProcessor.class);
        prometheusHttpConsumerMetrics = Mockito.mock(PrometheusHttpConsumerMetrics.class);
        elasticSearchCacheManager = Mockito.mock(ElasticSearchCacheManager.class);

        elasticSearchReader = new ElasticSearchReader(garmadonReaderBuilder, bulkProcessor,
                "garmadon-index", prometheusHttpConsumerMetrics, elasticSearchCacheManager);

        header = EventHeaderProtos.Header.newBuilder()
                .setUsername("user")
                .setApplicationId("app_id")
                .setApplicationName("application_name")
                .setAttemptId("attempt_id")
                .setContainerId("container_id")
                .build();

        headerMap.put("pid", "");
        headerMap.put("main_class", "");
        headerMap.put("application_id", "app_id");
        headerMap.put("tags", new ArrayList<>());
        headerMap.put("hostname", "");
        headerMap.put("component", "");
        headerMap.put("application_name", "application_name");
        headerMap.put("framework", "");
        headerMap.put("attempt_id", "attempt_id");
        headerMap.put("container_id", "container_id");
        headerMap.put("username", "user");
        headerMap.put("executor_id", "");
        headerMap.put("timestamp", 0);
    }

    public void writeGarmadonMessage(int type, Message message, long timestampMillis) {
        GarmadonMessage garmadonMessage = new GarmadonMessage(type, timestampMillis, header, message, null);
        elasticSearchReader.writeToES(garmadonMessage);
    }

    @Test
    public void writeToES_StateEventMessage() {
        int type = 3;
        String state = State.NEW.name();

        DataAccessEventProtos.StateEvent event = DataAccessEventProtos.StateEvent.newBuilder()
                .setState(state)
                .build();

        Map<String, Object> eventMap = new HashMap<>();
        eventMap.putAll(headerMap);
        eventMap.put("event_type", GarmadonSerialization.getTypeName(type));
        eventMap.put("state", state);

        writeGarmadonMessage(type, event, 0L);
        verify(bulkProcessor, times(1)).add(argument.capture(), any(CommittableOffset.class));

        assertEquals(eventMap, argument.getValue().sourceAsMap());
    }

    @Test
    public void writeToES_AppEventMessage() {
        int type = 4000;
        String state = State.NEW.name();

        ResourceManagerEventProtos.ApplicationEvent event = ResourceManagerEventProtos.ApplicationEvent.newBuilder()
                .setQueue("dev")
                .setTrackingUrl("http:/garmadon/test")
                .setState(state)
                .build();

        Map<String, Object> eventMap = new HashMap<>(headerMap);
        eventMap.put("event_type", GarmadonSerialization.getTypeName(type));
        eventMap.put("state", state);
        eventMap.put("queue", "dev");
        eventMap.put("tracking_url", "http:/garmadon/test");
        eventMap.put("original_tracking_url", "");
        eventMap.put("am_container_id", "");
        eventMap.put("yarn_tags", new ArrayList<>());
        eventMap.put("project_name", "");
        eventMap.put("workflow_name", "");

        writeGarmadonMessage(type, event, 0L);
        verify(bulkProcessor, times(1)).add(argument.capture(), any(CommittableOffset.class));

        assertEquals(eventMap, argument.getValue().sourceAsMap());
    }

    @Test
    public void writeToES_JVMstats() {
        int type = 1001;

        // Add disks metrics
        JVMStatisticsEventsProtos.JVMStatisticsData.Property propertyRx = JVMStatisticsEventsProtos.JVMStatisticsData.Property.newBuilder()
                .setName("sda_rx")
                .setValue("10")
                .build();
        JVMStatisticsEventsProtos.JVMStatisticsData.Property propertyTx = JVMStatisticsEventsProtos.JVMStatisticsData.Property.newBuilder()
                .setName("sda_tx")
                .setValue("10")
                .build();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section sectionDisk = JVMStatisticsEventsProtos.JVMStatisticsData.Section.newBuilder()
                .setName("disk")
                .addProperty(propertyRx)
                .addProperty(propertyTx)
                .build();

        // ADD JVM heap metric
        JVMStatisticsEventsProtos.JVMStatisticsData.Property property = JVMStatisticsEventsProtos.JVMStatisticsData.Property.newBuilder()
                .setName("heap_usage")
                .setValue("10")
                .build();
        JVMStatisticsEventsProtos.JVMStatisticsData.Section sectionHeap = JVMStatisticsEventsProtos.JVMStatisticsData.Section.newBuilder()
                .setName("jvm")
                .addProperty(property)
                .build();

        JVMStatisticsEventsProtos.JVMStatisticsData event = JVMStatisticsEventsProtos.JVMStatisticsData.newBuilder()
                .addSection(sectionDisk)
                .addSection(sectionHeap)
                .build();

        Map<String, Object> jvmEventMap = new HashMap<>();
        jvmEventMap.putAll(headerMap);
        jvmEventMap.put("event_type", "JVMSTATS_EVENT");
        jvmEventMap.put("jvm_heap_usage", 10.0);

        Map<String, Object> dsikEventMap = new HashMap<>();
        dsikEventMap.putAll(headerMap);
        dsikEventMap.put("event_type", "OS");
        dsikEventMap.put("disk", "sda");
        dsikEventMap.put("rx", 10.0);
        dsikEventMap.put("tx", 10.0);

        writeGarmadonMessage(type, event, 0L);
        verify(bulkProcessor, times(2)).add(argument.capture(), any(CommittableOffset.class));

        Map<String, Object> diskMap = null;
        Map<String, Object> jvmMap = null;
        for (IndexRequest indexRequest : argument.getAllValues()) {
            if (indexRequest.sourceAsMap().get("event_type").equals("OS")) {
                diskMap = indexRequest.sourceAsMap();
            } else if (indexRequest.sourceAsMap().get("event_type").equals("JVMSTATS_EVENT")) {
                jvmMap = indexRequest.sourceAsMap();
            }
        }
        assertEquals(dsikEventMap, diskMap);
        assertEquals(jvmEventMap, jvmMap);
    }

    @Test
    public void writeToES_FsEventMessage() {
        int type = 1;

        DataAccessEventProtos.FsEvent event = DataAccessEventProtos.FsEvent.newBuilder()
                .setAction(FsAction.WRITE.name())
                .setDstPath("hdfs://data:8020/var/test/val.lz4")
                .setUri("hdfs://data:8020")
                .setHdfsUser("lakeprobes")
                .setMethodDurationMillis(100L)
                .build();

        Map<String, Object> eventMap = new HashMap<>();
        eventMap.putAll(headerMap);
        eventMap.put("event_type", GarmadonSerialization.getTypeName(type));
        eventMap.put("action", "WRITE");
        eventMap.put("dst_path", "/var/test/val.lz4");
        eventMap.put("src_path", "");
        eventMap.put("uri", "hdfs://data-preprod-pa4");
        eventMap.put("method_duration_millis", 100);
        eventMap.put("hdfs_user", "lakeprobes");
        eventMap.put("status", "UNKNOWN");

        writeGarmadonMessage(type, event, 0L);
        verify(bulkProcessor, times(1)).add(argument.capture(), any(CommittableOffset.class));

        assertEquals(eventMap, argument.getValue().sourceAsMap());
    }

}
