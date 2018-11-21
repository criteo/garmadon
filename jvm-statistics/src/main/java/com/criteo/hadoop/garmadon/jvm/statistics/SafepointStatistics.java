package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import sun.management.HotspotRuntimeMBean;

class SafepointStatistics extends AbstractStatistic {
    private static final String SAFEPOINTS_HEADER = "safepoints";
    private static final String SAFEPOINTS_VAR_COUNT = "count";
    private static final String SAFEPOINTS_VAR_SYNCTIME = "synctime";
    private static final String SAFEPOINTS_VAR_TOTALTIME = "totaltime";

    private final HotspotRuntimeMBean hsRuntime;

    SafepointStatistics(HotspotRuntimeMBean hsRuntime) {
        super(SAFEPOINTS_HEADER);
        this.hsRuntime = hsRuntime;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) throws Throwable {
        sink.add(SAFEPOINTS_VAR_COUNT, hsRuntime.getSafepointCount());
        sink.add(SAFEPOINTS_VAR_SYNCTIME, hsRuntime.getSafepointSyncTime());
        sink.add(SAFEPOINTS_VAR_TOTALTIME, hsRuntime.getTotalSafepointTime());
    }
}