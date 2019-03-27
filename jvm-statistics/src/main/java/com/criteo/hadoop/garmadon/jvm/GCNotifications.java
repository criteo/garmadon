package com.criteo.hadoop.garmadon.jvm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ListenerNotFoundException;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.function.BiConsumer;

public class GCNotifications {
    private static final Logger LOGGER = LoggerFactory.getLogger(GCNotifications.class);

    private final NotificationListener listener;

    public GCNotifications(NotificationListener listener) {
        this.listener = listener;
    }

    public void subscribe(BiConsumer<Long, ?> printer) {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            NotificationEmitter emitter = (NotificationEmitter) bean;
            emitter.addNotificationListener(listener, null, printer);
        }
    }

    public void unsubscribe() {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            NotificationEmitter emitter = (NotificationEmitter) bean;
            try {
                emitter.removeNotificationListener(listener);
            } catch (ListenerNotFoundException e) {
                LOGGER.debug("Error during unsuscribing GC listener", e);
            }
        }
    }
}
