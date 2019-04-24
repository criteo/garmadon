package com.criteo.hadoop.garmadon.elasticsearch.cache;

import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.schema.enums.Component;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ElasticSearchCacheManager {

    protected final Cache<String, AppEventEnrichment> cacheAppEvent = CacheBuilder.newBuilder()
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .build();
    protected final Cache<String, String> cacheContainerComponent = CacheBuilder.newBuilder()
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .build();

    public void cacheEnrichableData(GarmadonMessage msg) {
        if (GarmadonSerialization.TypeMarker.APPLICATION_EVENT == msg.getType()) addAppEventInCache(msg);
        addContainerComponentInCache(msg);
    }

    // We cache some information from APPLICATION_EVENT to enrich all events from an app with
    // app_name, framework, am container, yarn tags
    protected void addAppEventInCache(GarmadonMessage msg) {
        if (cacheAppEvent.getIfPresent(msg.getHeader().getApplicationId()) == null) {
            ResourceManagerEventProtos.ApplicationEvent body = (ResourceManagerEventProtos.ApplicationEvent) msg.getBody();

            AppEventEnrichment appEvent = new AppEventEnrichment(msg.getHeader().getApplicationName(), msg.getHeader().getFramework(),
                body.getAmContainerId(), msg.getHeader().getUsername(), body.getYarnTagsList());
            cacheAppEvent.put(msg.getHeader().getApplicationId(), appEvent);
        }
    }

    protected void addContainerComponentInCache(GarmadonMessage msg) {
        addContainerComponentInCache(msg.getHeader().getContainerId(), msg.getHeader().getComponent());
    }

    private void addContainerComponentInCache(String containerId, String component) {
        if (cacheContainerComponent.getIfPresent(containerId) == null && !Component.UNKNOWN.name().equals(component) && !"".equals(component)) {
            cacheContainerComponent.put(containerId, component);
        }
    }

    public void enrichEvent(Map<String, Object> eventMap) {
        // Only enrich events from application
        String applicationId = (String) eventMap.get("application_id");
        if ("".equals(applicationId)) return;

        AppEventEnrichment appEvent = this.cacheAppEvent.getIfPresent(applicationId);
        // No AppEvent is available for this application id in the cache
        if (appEvent == null) return;

        enrichEventWithAppEvent(eventMap, appEvent);
        enrichEventWithComponent(eventMap, appEvent);
    }

    private void enrichEventWithAppEvent(Map<String, Object> eventMap, AppEventEnrichment appEvent) {
        // Enrich event
        eventMap.put("application_name", appEvent.getApplicationName());
        eventMap.put("framework", appEvent.getFramework());
        eventMap.put("username", appEvent.getUsername());
        eventMap.put("yarn_tags", appEvent.getYarnTags());
    }


    private void enrichEventWithComponent(Map<String, Object> eventMap, AppEventEnrichment appEvent) {
        // Only enrich container events
        String containerId = (String) eventMap.get("container_id");
        if ("".equals(containerId)) return;

        String component = this.cacheContainerComponent.getIfPresent(containerId);
        // No component is available for this container id in the cache
        if (component != null) {
            eventMap.put("component", component);
        } else if (Component.UNKNOWN.name().equals(eventMap.get("component"))) {
            if (containerId.equals(appEvent.getAmContainerId())) {
                setComponent(eventMap, containerId, Component.APP_MASTER.name());
            } else {
                setComponent(eventMap, containerId, Component.APP_SLAVE.name());
            }
        }
    }

    private void setComponent(Map<String, Object> eventMap, String containerId, String component) {
        eventMap.put("component", component);
        this.addContainerComponentInCache(containerId, component);
    }
}
