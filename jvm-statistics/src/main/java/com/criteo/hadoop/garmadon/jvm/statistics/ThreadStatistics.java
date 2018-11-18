package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;

import java.lang.management.ThreadMXBean;

class ThreadStatistics extends AbstractStatistic {
    private static final String THREADS_HEADER = "threads";
    private static final String THREADS_VAR_COUNT = "count";
    private static final String THREADS_VAR_DAEMON = "daemon";
    private static final String THREADS_VAR_TOTAL = "total";

    private final ThreadMXBean thread;

    ThreadStatistics(ThreadMXBean thread) {
        super(THREADS_HEADER);
        this.thread = thread;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        sink.add(THREADS_VAR_COUNT, thread.getThreadCount());
        sink.add(THREADS_VAR_DAEMON, thread.getDaemonThreadCount());
        sink.add(THREADS_VAR_TOTAL, thread.getTotalStartedThreadCount());
    }
}

