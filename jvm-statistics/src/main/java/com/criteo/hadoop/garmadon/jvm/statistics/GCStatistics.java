package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;

import java.lang.management.GarbageCollectorMXBean;

class GCStatistics extends AbstractStatistic {
    private static final String GC_HEADER = "gc";
    private static final String GC_VAR_COUNT = "count";
    private static final String GC_VAR_CPU = "cpu";

    private final int processors;
    private long cpu;
    private long time = System.currentTimeMillis();

    private final GarbageCollectorMXBean gc;

    GCStatistics(GarbageCollectorMXBean gc, int processors) {
        super(GC_HEADER + "(" + gc.getName() + ")");
        this.gc = gc;
        this.processors = processors;
        cpu = gc.getCollectionTime();
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        sink.add(GC_VAR_COUNT, gc.getCollectionCount());

        long currentTime = System.currentTimeMillis();
        long currentCpu = gc.getCollectionTime();
        if (currentTime != time) {
            float cpuPercent = computeCpuPercentage(cpu, currentCpu, processors, time, currentTime);
            cpu = currentCpu;
            time = currentTime;
            sink.addPercentage(GC_VAR_CPU, cpuPercent);
        }
    }
}

