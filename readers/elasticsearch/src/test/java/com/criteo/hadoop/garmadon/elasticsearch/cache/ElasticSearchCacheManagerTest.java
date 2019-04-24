package com.criteo.hadoop.garmadon.elasticsearch.cache;


import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.schema.enums.Component;
import com.criteo.hadoop.garmadon.schema.enums.FsAction;
import com.criteo.hadoop.garmadon.schema.enums.State;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.protobuf.Message;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ElasticSearchCacheManagerTest {
    private final String applicationId = "app_id";
    private final String containerId = "container_id";
    private final String adminContainerId = "container_id_1";
    private final String component = "EXECUTOR";
    private final String applicationName = "application_name";
    private final String framework = "SPARK";
    private final String username = "n.fraison";
    private final List<String> yarnTags = new ArrayList();

    private ElasticSearchCacheManager elasticSearchCacheManager;

    @Before
    public void setUp() {
        elasticSearchCacheManager = new ElasticSearchCacheManager();
    }

    private Map<String, Object> createJsonMap(String applicationId, String containerId, String component) {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("pid", "");
        eventMap.put("main_class", "");
        eventMap.put("application_id", applicationId);
        eventMap.put("tags", new ArrayList<>());
        eventMap.put("hostname", "");
        eventMap.put("component", component);
        eventMap.put("framework", "");
        eventMap.put("attempt_id", "attempt_id");
        eventMap.put("container_id", containerId);
        eventMap.put("username", "yarn");
        eventMap.put("executor_id", "");
        eventMap.put("timestamp", 0);
        eventMap.put("event_type", GarmadonSerialization.getTypeName(1));
        eventMap.put("state", State.RUNNING.name());
        eventMap.put("queue", "dev");
        eventMap.put("tracking_url", "http:/garmadon/test");
        eventMap.put("original_tracking_url", "");
        eventMap.put("am_container_id", "");
        return eventMap;
    }

    private ResourceManagerEventProtos.ApplicationEvent createApplicationEvent() {
        return ResourceManagerEventProtos.ApplicationEvent.newBuilder()
            .setQueue("dev")
            .setTrackingUrl("http:/garmadon/test")
            .setAmContainerId(adminContainerId)
            .addAllYarnTags(yarnTags)
            .setState(State.NEW.name())
            .build();
    }

    private DataAccessEventProtos.FsEvent createFsEvent() {
        return DataAccessEventProtos.FsEvent.newBuilder()
            .setAction(FsAction.WRITE.name())
            .setDstPath("hdfs://data:8020/var/test/val.lz4")
            .setUri("hdfs://data:8020")
            .setHdfsUser("lakeprobes")
            .setMethodDurationMillis(100L)
            .build();
    }

    private GarmadonMessage generateGarmadonMessage(int type, String component, Message body) {
        EventHeaderProtos.Header header = EventHeaderProtos.Header.newBuilder()
            .setUsername(username)
            .setApplicationId(applicationId)
            .setApplicationName(applicationName)
            .setAttemptId("attempt_id")
            .setContainerId(containerId)
            .setFramework(framework)
            .setComponent(component)
            .build();

        return new GarmadonMessage(type, 0L, header, body, null);
    }

    @Test
    public void add_app_event_in_cache() {
        ResourceManagerEventProtos.ApplicationEvent body = createApplicationEvent();
        GarmadonMessage msgAppEvent = generateGarmadonMessage(4000, component, body);

        AppEventEnrichment appEvent = new AppEventEnrichment(msgAppEvent.getHeader().getApplicationName(), msgAppEvent.getHeader().getFramework(),
            body.getAmContainerId(), msgAppEvent.getHeader().getUsername(), body.getYarnTagsList());

        // Check that applicationId is not in the cache
        assertNull(elasticSearchCacheManager.cacheAppEvent.getIfPresent(applicationId));

        elasticSearchCacheManager.cacheEnrichableData(msgAppEvent);

        // Check that applicationId has well been inserted in the cache
        assertEquals(appEvent, elasticSearchCacheManager.cacheAppEvent.getIfPresent(applicationId));
        assertEquals(1, elasticSearchCacheManager.cacheAppEvent.size());
    }

    @Test
    public void add_container_component_in_cache() {
        DataAccessEventProtos.FsEvent body = createFsEvent();
        GarmadonMessage msgFsEvent = generateGarmadonMessage(1, component, body);

        // Check that containerId is not in the cache
        assertNull(elasticSearchCacheManager.cacheContainerComponent.getIfPresent(containerId));

        elasticSearchCacheManager.cacheEnrichableData(msgFsEvent);

        // Check that containerId has well been inserted in the cache
        assertEquals(component, elasticSearchCacheManager.cacheContainerComponent.getIfPresent(containerId));
    }

    @Test
    public void do_not_add_container_with_empty_component_in_cache() {
        DataAccessEventProtos.FsEvent body = createFsEvent();
        GarmadonMessage msgFsEvent = generateGarmadonMessage(1, "", body);


        elasticSearchCacheManager.cacheEnrichableData(msgFsEvent);

        assertNull(elasticSearchCacheManager.cacheContainerComponent.getIfPresent(containerId));
    }

    @Test
    public void do_not_add_container_with_unknown_component_in_cache() {
        EventHeaderProtos.Header header = EventHeaderProtos.Header.newBuilder()
            .setUsername(username)
            .setApplicationId(applicationId)
            .setApplicationName(applicationName)
            .setAttemptId("attempt_id")
            .setContainerId(containerId)
            .setComponent(Component.UNKNOWN.name())
            .setFramework(framework)
            .build();

        DataAccessEventProtos.FsEvent fsEvent = DataAccessEventProtos.FsEvent.newBuilder()
            .setAction(FsAction.WRITE.name())
            .setDstPath("hdfs://data:8020/var/test/val.lz4")
            .setUri("hdfs://data:8020")
            .setHdfsUser("lakeprobes")
            .setMethodDurationMillis(100L)
            .build();

        elasticSearchCacheManager.cacheEnrichableData(new GarmadonMessage(1, 0L, header, fsEvent, null));

        assertNull(elasticSearchCacheManager.cacheContainerComponent.getIfPresent(containerId));
    }

    @Test
    public void enrich_event_with_app_in_the_cache() {
        ResourceManagerEventProtos.ApplicationEvent appEvent = createApplicationEvent();
        GarmadonMessage msgAppEvent = generateGarmadonMessage(4000, component, appEvent);
        DataAccessEventProtos.FsEvent fsEvent = createFsEvent();
        GarmadonMessage msgFsEvent = generateGarmadonMessage(1, component, fsEvent);

        elasticSearchCacheManager.cacheEnrichableData(msgAppEvent);
        elasticSearchCacheManager.cacheEnrichableData(msgFsEvent);

        Map<String, Object> eventMap = createJsonMap(applicationId, containerId, "");

        elasticSearchCacheManager.enrichEvent(eventMap);

        assertEquals(applicationName, eventMap.get("application_name"));
        assertEquals(framework, eventMap.get("framework"));
        assertEquals(username, eventMap.get("username"));
        assertEquals(yarnTags, eventMap.get("yarn_tags"));
        assertEquals(component, eventMap.get("component"));
    }

    @Test
    public void enrich_unknown_component_event_with_app_in_the_cache() {
        ResourceManagerEventProtos.ApplicationEvent appEvent = createApplicationEvent();
        GarmadonMessage msgAppEvent = generateGarmadonMessage(4000, component, appEvent);
        DataAccessEventProtos.FsEvent fsEvent = createFsEvent();
        GarmadonMessage msgFsEvent = generateGarmadonMessage(1, component, fsEvent);

        String unknownContainerId = containerId + "_2";
        Map<String, Object> eventMap = createJsonMap(applicationId, unknownContainerId, Component.UNKNOWN.name());

        elasticSearchCacheManager.cacheEnrichableData(msgAppEvent);
        elasticSearchCacheManager.cacheEnrichableData(msgFsEvent);

        elasticSearchCacheManager.enrichEvent(eventMap);

        assertEquals(applicationName, eventMap.get("application_name"));
        assertEquals(framework, eventMap.get("framework"));
        assertEquals(username, eventMap.get("username"));
        assertEquals(yarnTags, eventMap.get("yarn_tags"));
        assertEquals(Component.APP_SLAVE.name(), eventMap.get("component"));
    }

    @Test
    public void enrich_am_unknown_component_event_with_app_in_the_cache() {
        ResourceManagerEventProtos.ApplicationEvent appEvent = createApplicationEvent();
        GarmadonMessage msgAppEvent = generateGarmadonMessage(4000, component, appEvent);
        DataAccessEventProtos.FsEvent fsEvent = createFsEvent();
        GarmadonMessage msgFsEvent = generateGarmadonMessage(1, component, fsEvent);

        Map<String, Object> eventMap = createJsonMap(applicationId, adminContainerId, Component.UNKNOWN.name());

        elasticSearchCacheManager.cacheEnrichableData(msgAppEvent);
        elasticSearchCacheManager.cacheEnrichableData(msgFsEvent);

        elasticSearchCacheManager.enrichEvent(eventMap);

        assertEquals(applicationName, eventMap.get("application_name"));
        assertEquals(framework, eventMap.get("framework"));
        assertEquals(username, eventMap.get("username"));
        assertEquals(yarnTags, eventMap.get("yarn_tags"));
        assertEquals(Component.APP_MASTER.name(), eventMap.get("component"));
    }

    @Test
    public void do_not_enrich_event_with_app_not_in_the_cache() {
        ResourceManagerEventProtos.ApplicationEvent appEvent = createApplicationEvent();
        GarmadonMessage msgAppEvent = generateGarmadonMessage(4000, component, appEvent);
        DataAccessEventProtos.FsEvent fsEvent = createFsEvent();
        GarmadonMessage msgFsEvent = generateGarmadonMessage(1, component, fsEvent);

        Map<String, Object> eventMap = createJsonMap(applicationId + "_1", containerId, "");
        elasticSearchCacheManager.cacheEnrichableData(msgAppEvent);
        elasticSearchCacheManager.cacheEnrichableData(msgFsEvent);

        elasticSearchCacheManager.enrichEvent(eventMap);

        assertNull(applicationName, eventMap.get("application_name"));
    }

}
