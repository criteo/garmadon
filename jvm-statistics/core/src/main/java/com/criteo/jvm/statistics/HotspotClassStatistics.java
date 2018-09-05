package com.criteo.jvm.statistics;

import com.criteo.jvm.StatisticsSink;
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
        if (initialized != -1) // Azul VM does not provide times
            sink.add(CLASS_VAR_INITIALIZED, initialized);
        addValidDuration(sink, CLASS_VAR_LOADTIME, hsClassloading.getClassLoadingTime());
        addValidDuration(sink, CLASS_VAR_INITTIME, hsClassloading.getClassInitializationTime());
        addValidDuration(sink, CLASS_VAR_VERIFTIME, hsClassloading.getClassVerificationTime());
    }

    private static void addValidDuration(StatisticsSink sink, String property, long duration) {
        if (duration == -1) // Azul VM does not provide times
            return;
        sink.addDuration(property, duration);
    }
}

