package com.criteo.jvm.statistics;

import com.criteo.jvm.StatisticCollector;

public class MachineStatistics {
    public void register(StatisticCollector collector) {
        collector.register(new MachineCpuStatistics());
        collector.register(new MemoryStatistics());
        collector.register(new NetworkStatistics());
        collector.register(new DiskStatistics());
    }
}
