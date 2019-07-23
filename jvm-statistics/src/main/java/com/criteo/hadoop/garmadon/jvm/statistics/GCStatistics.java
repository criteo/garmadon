package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;

import java.lang.management.GarbageCollectorMXBean;

class GCStatistics extends AbstractStatistic {
    private static final String GC_HEADER = "gc";
    private static final String GC_VAR_COUNT = "count";
    private static final String GC_VAR_TIME = "time";

    private final GarbageCollectorMXBean gc;

    GCStatistics(GarbageCollectorMXBean gc) {
        super(GC_HEADER + "(" + gc.getName() + ")");
        this.gc = gc;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        sink.add(GC_VAR_COUNT, gc.getCollectionCount());
        sink.addDuration(GC_VAR_TIME, gc.getCollectionTime());
    }
}

