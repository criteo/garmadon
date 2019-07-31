package com.criteo.hadoop.garmadon.agent.tracers.hadoop.resourcemanager;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos.ApplicationEvent;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class RMContextImplEventRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RMContextImplEventRunnable.class);

    private static final Map<String, BiConsumer<String, ApplicationEvent.Builder>> BUILDERS;
    private static final Set<String> YARN_TAGS_TO_EXTRACT;

    static {
        BUILDERS = new HashMap<>();
        BUILDERS.put("garmadon.project.name", (value, builder) -> builder.setProjectName(value));
        BUILDERS.put("garmadon.workflow.name", (value, builder) -> builder.setWorkflowName(value));

        YARN_TAGS_TO_EXTRACT = BUILDERS.keySet();
    }

    // Cache used to avoid sending FINISHED/KILLED state event multiple times
    // as we iterate on all apps referenced by RM even finished one until they are
    // evicted by the RM
    private final Cache<String, String> cacheFinishedApp = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .build();

    private final RMContextImpl rmContext;
    private final TriConsumer<Long, Header, Object> eventHandler;

    public RMContextImplEventRunnable(RMContextImpl rmContext, TriConsumer<Long, Header, Object> eventHandler) {
        this.rmContext = rmContext;
        this.eventHandler = eventHandler;
    }

    public String normalizeTrackingUrl(String trackingUrl) {
        return !trackingUrl.contains("http://") && !trackingUrl.contains("https://") ? "http://" + trackingUrl : trackingUrl;
    }

    public void sendAppEvent(ApplicationId applicationId, RMApp rmApp) {
        if (cacheFinishedApp.getIfPresent(applicationId.toString()) == null) {
            Header.Builder headerBuilder = Header.newBuilder()
                .withId(applicationId.toString())
                .withApplicationID(applicationId.toString())
                .withUser(rmApp.getUser())
                .withApplicationName(rmApp.getName())
                .withFramework(rmApp.getApplicationType().toUpperCase());

            ApplicationEvent.Builder eventBuilder = ApplicationEvent.newBuilder()
                .setState(rmApp.getState().name())
                .setQueue(rmApp.getQueue());

            rmApp.getApplicationTags().stream()
                .filter(tag -> YARN_TAGS_TO_EXTRACT.stream().noneMatch(tag::startsWith) && !tag.contains(":"))
                .forEach(eventBuilder::addYarnTags);

            rmApp.getApplicationTags().stream()
                .filter(tag -> tag.contains(":") && YARN_TAGS_TO_EXTRACT.stream().anyMatch(tag::startsWith))
                .map(tag -> {
                    int idx = tag.indexOf(':');
                    String key = tag.substring(0, idx);
                    String value = tag.substring(idx + 1);
                    return new String[] {key, value};
                })
                .forEach(splitTag -> BUILDERS.get(splitTag[0]).accept(splitTag[1], eventBuilder));

            eventBuilder.setFinalStatus(rmApp.getFinalApplicationStatus().name());
            eventBuilder.setStartTime(rmApp.getStartTime());
            eventBuilder.setFinishTime(rmApp.getFinishTime());

            RMAppMetrics rmAppMetrics = rmApp.getRMAppMetrics();
            if (rmAppMetrics != null) {
                eventBuilder.setMemorySeconds(rmAppMetrics.getMemorySeconds());
                eventBuilder.setVcoreSeconds(rmAppMetrics.getVcoreSeconds());
            }

            RMAppAttempt rmAppAttempt = rmApp.getCurrentAppAttempt();
            if (rmAppAttempt != null) {
                headerBuilder.withAttemptID(rmAppAttempt.getAppAttemptId().toString());

                Container container = rmAppAttempt.getMasterContainer();
                if (container != null) {
                    eventBuilder.setAmContainerId(container.getId().toString());
                }
            }

            if (rmApp.getTrackingUrl() != null) {
                eventBuilder.setTrackingUrl(normalizeTrackingUrl(rmApp.getTrackingUrl()));
            }

            if (rmApp.getOriginalTrackingUrl() != null && !"N/A".equals(rmApp.getOriginalTrackingUrl())) {
                eventBuilder.setOriginalTrackingUrl(normalizeTrackingUrl(rmApp.getOriginalTrackingUrl()));
            }

            eventHandler.accept(System.currentTimeMillis(), headerBuilder.build(), eventBuilder.build());

            if (rmApp.getState() == RMAppState.FINISHED || rmApp.getState() == RMAppState.KILLED || rmApp.getState() == RMAppState.FAILED) {
                cacheFinishedApp.put(applicationId.toString(), rmApp.getState().name());
            }
        }
    }

    @Override
    public void run() {
        try {
            if (rmContext != null && rmContext.getRMApps() != null) {
                // Get Apps info
                rmContext.getRMApps().forEach((applicationId, rmApp) -> {
                    try {
                        sendAppEvent(applicationId, rmApp);
                    } catch (Exception ex) {
                        LOGGER.error("Failed to generate APPLICATION_EVENT for " + applicationId, ex);
                    }
                });
            }
        } catch (Throwable ignored) {
        }
    }
}
