package com.criteo.hadoop.garmadon.agent.tracers.hadoop.resourcemanager;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RMContextImplEventRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RMContextImplEventRunnable.class);

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

            ResourceManagerEventProtos.ApplicationEvent.Builder eventBuilder = ResourceManagerEventProtos.ApplicationEvent.newBuilder()
                .setState(rmApp.getState().name())
                .setQueue(rmApp.getQueue())
                .addAllYarnTags(rmApp.getApplicationTags());

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

            if (rmApp.getState() == RMAppState.FINISHED || rmApp.getState() == RMAppState.KILLED) {
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
        } catch (Exception ignored) {
        }
    }
}
