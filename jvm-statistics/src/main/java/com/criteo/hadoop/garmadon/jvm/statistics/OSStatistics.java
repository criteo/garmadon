package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;

import java.lang.management.OperatingSystemMXBean;

class OSStatistics extends AbstractStatistic {
    private static final String OS_HEADER = "os";
    private static final String OS_VAR_LOADAVG = "loadavg";

    private final OperatingSystemMXBean os;
    private final int processors;

    OSStatistics(OperatingSystemMXBean os, int processors) {
        super(OS_HEADER);
        this.os = os;
        this.processors = processors;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        if (isLoadAverageAvailable()) {
            sink.addPercentage(OS_VAR_LOADAVG, (int) (100 * os.getSystemLoadAverage() / processors));
        }
    }

    private boolean isLoadAverageAvailable() {
        return os.getSystemLoadAverage() >= 0;
    }
}
