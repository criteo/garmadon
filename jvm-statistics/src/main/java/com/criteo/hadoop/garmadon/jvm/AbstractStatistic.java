package com.criteo.hadoop.garmadon.jvm;

import com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;

/**
 * Base class for any specific statistic
 */
public abstract class AbstractStatistic implements Closeable {
    private final String name;

    public AbstractStatistic(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void collect(StatisticsSink sink) throws Throwable {
        sink.beginSection(name);
        innerCollect(sink);
        sink.endSection();
    }

    protected abstract void innerCollect(StatisticsSink sink) throws Throwable;

    public void close() {

    }

    @VisibleForTesting
    public static float computeCpuPercentage(long prevCpu, long currentCpu, int processors, long prevTimeStamp, long currentTimeStamp) {
        return ((float) (currentCpu - prevCpu) / (currentTimeStamp - prevTimeStamp)) / processors * 100; // CPU percentage consumed during 2 collects
    }

    @Override
    public String toString() {
        return name;
    }
}