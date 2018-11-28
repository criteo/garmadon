package com.criteo.hadoop.garmadon.jvm;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.function.BiConsumer;

public class LogGCNotifications extends GCNotifications {
    private static final ThreadLocal<SimpleDateFormat> GC_TIME_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));

    public LogGCNotifications() {
        super(getNotificationListener());
    }

    static void handleNotification(Notification notification, Object handback) {
        BiConsumer<Long, String> printer = (BiConsumer<Long, String>) handback;
        printer.accept(System.currentTimeMillis(), "GC occurred");
    }

    static void handleHSNotification(Notification notification, Object handback) {
        BiConsumer<Long, String> printer = (BiConsumer<Long, String>) handback;
        GarbageCollectionNotificationInfo gcNotifInfo = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
        GcInfo gcInfo = gcNotifInfo.getGcInfo();
        long start = gcInfo.getStartTime();
        long end = gcInfo.getEndTime();
        StringBuilder sb = new StringBuilder();
        sb.append(gcNotifInfo.getGcName());
        sb.append(" occurred at ");
        long serverStartTime = ManagementFactory.getRuntimeMXBean().getStartTime();
        sb.append(GC_TIME_FORMAT.get().format(new Date(start + serverStartTime)));
        sb.append(", took ");
        sb.append(end - start);
        sb.append("ms");
        String cause = gcNotifInfo.getGcCause();
        if (cause != null) {
            sb.append(" (");
            sb.append(cause);
            sb.append(") ");
        }
        Map<String, MemoryUsage> memoryUsageBeforeGc = gcInfo.getMemoryUsageBeforeGc();
        Map<String, MemoryUsage> memoryUsageAfterGc = gcInfo.getMemoryUsageAfterGc();
        for (Map.Entry<String, MemoryUsage> entry : memoryUsageAfterGc.entrySet()) {
            MemoryUsage before = memoryUsageBeforeGc.get(entry.getKey());
            MemoryUsage after = entry.getValue();
            long usedDelta = (before != null) ? (after.getUsed() - before.getUsed()) / 1024 : 0;
            if (usedDelta != 0) {
                sb.append(" ");
                sb.append(MXBeanHelper.normalizeName(entry.getKey()));
                sb.append("[").append(usedDelta > 0 ? "+" : "").append(usedDelta).append("]");
                sb.append("(").append(before.getUsed() / 1024).append("->").append(after.getUsed() / 1024).append(")");
            }
        }
        printer.accept(start, sb.toString());
    }

    private static NotificationListener getNotificationListener() {
        try {
            Class.forName("com.sun.management.GarbageCollectionNotificationInfo");
            return LogGCNotifications::handleHSNotification;
        } catch (ClassNotFoundException ex) {
            return LogGCNotifications::handleNotification;
        }
    }

}
