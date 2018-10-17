package com.criteo.hadoop.garmadon.agent.tracers.hadoop.resourcemanager;

import com.criteo.hadoop.garmadon.TriConsumer;
import com.criteo.hadoop.garmadon.event.proto.ResourceManagerEventProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

import java.util.concurrent.TimeUnit;

public class RMContextImplEventRunnable implements Runnable {
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

    @Override
    public void run() {
        try {
            if (rmContext != null) {
                // Get Apps info
                rmContext.getRMApps().forEach((applicationId, rmApp) -> {
                    if (cacheFinishedApp.getIfPresent(applicationId.toString()) == null) {
                        Header header = Header.newBuilder()
                                .withApplicationID(applicationId.toString())
                                .withUser(rmApp.getUser())
                                .withApplicationName(rmApp.getName())
                                .withAppAttemptID(rmApp.getCurrentAppAttempt().getAppAttemptId().toString())
                                .withFramework(rmApp.getApplicationType().toUpperCase())
                                .build();

                        ResourceManagerEventProtos.ApplicationEvent.Builder eventBuilder = ResourceManagerEventProtos.ApplicationEvent.newBuilder()
                                .setState(rmApp.getState().name())
                                .setQueue(rmApp.getQueue())
                                .setTrackingUrl(normalizeTrackingUrl(rmApp.getTrackingUrl()));

                        if (rmApp.getOriginalTrackingUrl() != "N/A") {
                            eventBuilder.setOriginalTrackingUrl(normalizeTrackingUrl(rmApp.getOriginalTrackingUrl()));
                        }

                        eventHandler.accept(System.currentTimeMillis(), header, eventBuilder.build());

                        if (rmApp.getState() == RMAppState.FINISHED || rmApp.getState() == RMAppState.KILLED) {
                            cacheFinishedApp.put(applicationId.toString(), rmApp.getState().name());
                        }
                    }
                });
            }
        } catch (Exception ignored) {
        }
    }
}
