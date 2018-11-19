package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import sun.management.HotspotClassLoadingMBean;

import java.lang.management.ClassLoadingMXBean;

class HotspotClassStatistics extends ClassStatistics {
    private static final String CLASS_VAR_INITIALIZED = "initialized";
    private static final String CLASS_VAR_LOADTIME = "loadtime";
    private static final String CLASS_VAR_INITTIME = "inittime";
    private static final String CLASS_VAR_VERIFTIME = "veriftime";

    private final HotspotClassLoadingMBean hsClassloading;

    HotspotClassStatistics(ClassLoadingMXBean classloading, HotspotClassLoadingMBean hsClassloading) {
        super(classloading);
        this.hsClassloading = hsClassloading;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        super.innerCollect(sink);
        long initialized = hsClassloading.getInitializedClassCount();
        if (initialized != -1) sink.add(CLASS_VAR_INITIALIZED, initialized); // Azul VM does not provide times
        addValidDuration(sink, CLASS_VAR_LOADTIME, hsClassloading.getClassLoadingTime());
        addValidDuration(sink, CLASS_VAR_INITTIME, hsClassloading.getClassInitializationTime());
        addValidDuration(sink, CLASS_VAR_VERIFTIME, hsClassloading.getClassVerificationTime());
    }

    private static void addValidDuration(StatisticsSink sink, String property, long duration) {
        if (duration == -1) return; // Azul VM does not provide times
        sink.addDuration(property, duration);
    }
}

