package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.StatisticCollector;
import com.criteo.hadoop.garmadon.jvm.Conf;
import com.criteo.hadoop.garmadon.jvm.MXBeanHelper;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;

public class MXBeanStatistics {
    protected static final String HEAP_HEADER = "heap";
    protected static final String NON_HEAP_HEADER = "nonheap";

    private final Conf conf;

    public MXBeanStatistics(Conf conf) {
        this.conf = conf;
    }

    public void register(StatisticCollector collector) {
        addGCStatistics(collector);
        addMemoryStatistics(collector);
        addCompilationStatistics(collector);
        addThreadStatistics(collector);
        addClassStatistics(collector);
        addOSStatistics(collector);
    }

    protected void addGCStatistics(StatisticCollector collector) {
        GarbageCollectorMXBean[] garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans().toArray(new GarbageCollectorMXBean[0]);
        for (GarbageCollectorMXBean garbageCollector : garbageCollectors) {
            collector.register(new GCStatistics(garbageCollector));
        }
    }

    protected void addMemoryStatistics(StatisticCollector collector) {
        collector.register(new MemoryUsageStatistics(HEAP_HEADER, ManagementFactory.getMemoryMXBean()::getHeapMemoryUsage));
        collector.register(new MemoryUsageStatistics(NON_HEAP_HEADER, ManagementFactory.getMemoryMXBean()::getNonHeapMemoryUsage));
        MemoryPoolMXBean[] memPools = ManagementFactory.getMemoryPoolMXBeans().toArray(new MemoryPoolMXBean[0]);
        for (MemoryPoolMXBean memPool : memPools) {
            String poolName = MXBeanHelper.normalizeName(memPool.getName());
            collector.register(new MemoryUsageStatistics(poolName, memPool::getUsage));
        }
    }

    protected void addCompilationStatistics(StatisticCollector collector) {
        collector.register(new CompilationStatistics(ManagementFactory.getCompilationMXBean()));
    }

    protected void addThreadStatistics(StatisticCollector collector) {
        collector.register(new ThreadStatistics(ManagementFactory.getThreadMXBean()));
    }

    protected void addClassStatistics(StatisticCollector collector) {
        collector.register(new ClassStatistics(ManagementFactory.getClassLoadingMXBean()));
    }

    protected void addOSStatistics(StatisticCollector collector) {
        if (conf.isOsStatsInJvmStats()) { // under conf because can be redundant with machine stats
            OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            collector.register(new OSStatistics(os, os.getAvailableProcessors()));
        }
    }
}
