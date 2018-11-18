package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import com.google.common.annotations.VisibleForTesting;
import com.sun.management.OperatingSystemMXBean;

class CpuStatistics extends AbstractStatistic {
    private static final String CPU_HEADER = "cpu";
    private static final String CPU_VAR_LOAD = "load";
    private static final String CPU_VAR_CORES = "cores";

    private final OperatingSystemMXBean hosx;
    private final int processors;
    private long time = System.currentTimeMillis();
    private long cpu;

    CpuStatistics(OperatingSystemMXBean hosx, int processors) {
        super(CPU_HEADER);
        this.hosx = hosx;
        this.processors = processors;
        cpu = hosx.getProcessCpuTime();
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        sink.add(CPU_VAR_CORES, processors);
        long currentTime = System.currentTimeMillis();
        long currentCpu = hosx.getProcessCpuTime();
        if (currentTime != time) {
            long cpuPercent = computeCpuPercentage(cpu, currentCpu, processors, time, currentTime);
            cpu = currentCpu;
            time = currentTime;
            sink.addPercentage(CPU_VAR_LOAD, (int) cpuPercent);
        }
    }

    @VisibleForTesting
    static long computeCpuPercentage(long prevCpu, long currentCpu, int processors, long prevTimeStamp, long currentTimeStamp) {
        return (currentCpu - prevCpu) / 10000 / processors / (currentTimeStamp - prevTimeStamp); // CPU percentage consumed during 2 collects
    }
}

