package com.criteo.hadoop.garmadon.jvm;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class ProtobufGCNotifications extends GCNotifications {
    private static final long MILLIS_MINUTE = 60000;

    private static List<GcEvent> gcEvents = new ArrayList<>();


    public ProtobufGCNotifications() {
        super(getNotificationListener());
    }

    static void handleHSNotification(Notification notification, Object handback) {
        BiConsumer<Long, JVMStatisticsEventsProtos.GCStatisticsData> printer = (BiConsumer<Long, JVMStatisticsEventsProtos.GCStatisticsData>) handback;
        GarbageCollectionNotificationInfo gcNotifInfo = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
        GcInfo gcInfo = gcNotifInfo.getGcInfo();
        GcEvent gcEvent = new GcEvent(gcInfo.getStartTime(), gcInfo.getEndTime());
        long pauseTime = gcEvent.getPauseDuration();
        String collectorName = gcNotifInfo.getGcName();
        long serverStartTime = ManagementFactory.getRuntimeMXBean().getStartTime();
        long timestamp = gcInfo.getStartTime() + serverStartTime;
        String cause = gcNotifInfo.getGcCause();
        JVMStatisticsEventsProtos.GCStatisticsData.Builder builder = JVMStatisticsEventsProtos.GCStatisticsData.newBuilder();
        builder.setPauseTime(pauseTime);

        builder.setGcPauseRatio1Min((float) computeTotalPauseTime(gcEvents, gcEvent) / MILLIS_MINUTE * 100);
        gcEvents.add(gcEvent);

        builder.setCollectorName(collectorName);
        builder.setCause(cause);
        Map<String, MemoryUsage> memoryUsageBeforeGc = gcInfo.getMemoryUsageBeforeGc();
        Map<String, MemoryUsage> memoryUsageAfterGc = gcInfo.getMemoryUsageAfterGc();
        for (Map.Entry<String, MemoryUsage> entry : memoryUsageAfterGc.entrySet()) {
            MemoryUsage before = memoryUsageBeforeGc.get(entry.getKey());
            MemoryUsage after = entry.getValue();
            switch (MXBeanHelper.normalizeName(entry.getKey())) {
                case MXBeanHelper.MEMORY_POOL_CODE_HEADER:
                    builder.setCodeBefore(before.getUsed());
                    builder.setCodeAfter(after.getUsed());
                    break;
                case MXBeanHelper.MEMORY_POOL_PERM_HEADER:
                case MXBeanHelper.MEMORY_POOL_METASPACE_HEADER:
                    builder.setMetaspaceBefore(before.getUsed());
                    builder.setMetaspaceAfter(after.getUsed());
                    break;
                case MXBeanHelper.MEMORY_POOL_EDEN_HEADER:
                    builder.setEdenBefore(before.getUsed());
                    builder.setEdenAfter(after.getUsed());
                    break;
                case MXBeanHelper.MEMORY_POOL_SURVIVOR_HEADER:
                    builder.setSurvivorBefore(before.getUsed());
                    builder.setSurvivorAfter(after.getUsed());
                    break;
                case MXBeanHelper.MEMORY_POOL_OLD_HEADER:
                    builder.setOldBefore(before.getUsed());
                    builder.setOldAfter(after.getUsed());
                    break;
                case MXBeanHelper.MEMORY_POOL_COMPRESSEDCLASSPACE_HEADER:
                    // ignore
                    break;
                default:
                    throw new UnsupportedOperationException(entry.getKey() + " not supported");
            }
        }
        printer.accept(timestamp, builder.build());
    }

    protected static long computeTotalPauseTime(List<GcEvent> gcEvents, GcEvent gcEvent) {
        // Remove older gc events and compute total gc pause time since a minute
        long totalPauseTime = 0;
        long maxEndTimeMillis = gcEvent.getEndTime() - MILLIS_MINUTE;
        for (Iterator<GcEvent> it = gcEvents.iterator(); it.hasNext(); ) {
            GcEvent gEvent = it.next();
            if (gEvent.isTooOld(maxEndTimeMillis)) {
                it.remove();
            } else {
                totalPauseTime += gEvent.getPauseDurationSince(maxEndTimeMillis);
            }
        }
        totalPauseTime += gcEvent.getPauseDurationSince(maxEndTimeMillis);
        return totalPauseTime;
    }

    private static NotificationListener getNotificationListener() {
        try {
            Class.forName("com.sun.management.GarbageCollectionNotificationInfo");
            return ProtobufGCNotifications::handleHSNotification;
        } catch (ClassNotFoundException ex) {
            throw new UnsupportedOperationException("GC Notifications not supported");
        }
    }
}
