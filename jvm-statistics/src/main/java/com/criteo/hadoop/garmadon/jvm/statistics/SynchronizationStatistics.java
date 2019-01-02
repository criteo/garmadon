package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import sun.management.HotspotRuntimeMBean;
import sun.management.counter.Counter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

class SynchronizationStatistics extends AbstractStatistic {
    private static final String SYNCLOCKS_HEADER = "synclocks";

    private final Set<String> counters = new HashSet<>(Arrays.asList(
            "sun.rt._sync_ContendedLockAttempts", // number of time we try acquire a monitor but already acquired.
            "sun.rt._sync_FutileWakeups", // number of thread wakeups but still need to wait.
            "sun.rt._sync_Parks", // number of unparks = wakeup from wait state.
            "sun.rt._sync_Notifications", // number of notify send to threads (notifyAll increases it by number of threads waiting)
            "sun.rt._sync_Inflations", // locks inflated (ObjectMonitor allocation)
            "sun.rt._sync_Deflations", // locks deflated when idle (during safepoint)
            "sun.rt._sync_MonExtant" // block size used to store monitors
    ));
    private final HotspotRuntimeMBean hsRuntime;

    SynchronizationStatistics(HotspotRuntimeMBean hsRuntime) {
        super(SYNCLOCKS_HEADER);
        this.hsRuntime = hsRuntime;
        int countOfCounters = 0;
        for (Counter counter : hsRuntime.getInternalRuntimeCounters()) {
            String counterName = counter.getName();
            if (this.counters.contains(counterName)) countOfCounters++;
        }
        if (countOfCounters == 0) throw new RuntimeException("No sync locks counters found");
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        for (Counter counter : hsRuntime.getInternalRuntimeCounters()) {
            String counterName = counter.getName();
            if (counters.contains(counterName)) {
                counterName = counterName.substring("sun.rt._sync_".length()).toLowerCase();
                Object value = counter.getValue();
                sink.add(counterName, value.toString());
            }
        }
    }
}
