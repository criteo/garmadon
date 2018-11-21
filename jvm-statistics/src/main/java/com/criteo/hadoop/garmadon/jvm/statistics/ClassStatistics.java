package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;

import java.lang.management.ClassLoadingMXBean;

class ClassStatistics extends AbstractStatistic {
    private static final String CLASS_HEADER = "class";
    private static final String CLASS_VAR_LOADED = "loaded";
    private static final String CLASS_VAR_UNLOADED = "unloaded";

    private final ClassLoadingMXBean classloading;

    ClassStatistics(ClassLoadingMXBean classloading) {
        super(CLASS_HEADER);
        this.classloading = classloading;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        sink.add(CLASS_VAR_LOADED, classloading.getLoadedClassCount());
        sink.add(CLASS_VAR_UNLOADED, classloading.getUnloadedClassCount());
    }
}

