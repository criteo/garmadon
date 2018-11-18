package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import sun.management.HotspotThreadMBean;

import java.lang.management.ThreadMXBean;

class HotspotThreadStatistics extends ThreadStatistics {
    private static final String THREADS_VAR_INTERNAL = "internal";

    private final HotspotThreadMBean hsThread;

    HotspotThreadStatistics(ThreadMXBean thread, HotspotThreadMBean hsThread) {
        super(thread);
        this.hsThread = hsThread;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        super.innerCollect(sink);
        sink.add(THREADS_VAR_INTERNAL, hsThread.getInternalThreadCount());
    }
}

