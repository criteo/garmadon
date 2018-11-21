package com.criteo.hadoop.garmadon.jvm;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.function.BiConsumer;

public class ProtobufGCNotifications extends GCNotifications {

    public ProtobufGCNotifications() {
        super(getNotificationListener());
    }

    static void handleHSNotification(Notification notification, Object handback) {
        BiConsumer<Long, JVMStatisticsEventsProtos.GCStatisticsData> printer = (BiConsumer<Long, JVMStatisticsEventsProtos.GCStatisticsData>) handback;
        GarbageCollectionNotificationInfo gcNotifInfo = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
        GcInfo gcInfo = gcNotifInfo.getGcInfo();
        long pauseTime = gcInfo.getEndTime() - gcInfo.getStartTime();
        String collectorName = gcNotifInfo.getGcName();
        long serverStartTime = ManagementFactory.getRuntimeMXBean().getStartTime();
        long timestamp = gcInfo.getStartTime() + serverStartTime;
        String cause = gcNotifInfo.getGcCause();
        JVMStatisticsEventsProtos.GCStatisticsData.Builder builder = JVMStatisticsEventsProtos.GCStatisticsData.newBuilder();
        builder.setPauseTime(pauseTime);
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
                default: throw new UnsupportedOperationException(entry.getKey() + " not supported");
            }
        }
        printer.accept(timestamp, builder.build());
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
