package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;

class MemoryStatistics extends AbstractStatistic {
    private static final String MEMORY_HEADER = "memory";
    private static final String MEMORY_VAR_SWAP = "swap";
    private static final String MEMORY_VAR_PHYSICAL = "physical";

    private GlobalMemory globalMemory = new SystemInfo().getHardware().getMemory();

    MemoryStatistics() {
        super(MEMORY_HEADER);
    }

    @Override
    protected void innerCollect(StatisticsSink sink) throws Throwable {
        sink.addSize(MEMORY_VAR_SWAP, globalMemory.getSwapTotal() - globalMemory.getSwapUsed());
        sink.addSize(MEMORY_VAR_PHYSICAL, globalMemory.getAvailable());
    }
}

