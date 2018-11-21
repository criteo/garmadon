package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.AbstractStatistic;
import com.criteo.hadoop.garmadon.jvm.StatisticsSink;

import java.lang.management.MemoryUsage;
import java.util.function.Supplier;

class MemoryUsageStatistics extends AbstractStatistic {
    private static final String MEMORY_POOL_VAR_USED = "used";
    private static final String MEMORY_POOL_VAR_COMMITTED = "committed";
    private static final String MEMORY_POOL_VAR_INIT = "init";
    private static final String MEMORY_POOL_VAR_MAX = "max";

    private final Supplier<MemoryUsage> memUsageSupplier;

    MemoryUsageStatistics(String name, Supplier<MemoryUsage> memUsageSupplier) {
        super(name);
        this.memUsageSupplier = memUsageSupplier;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        MemoryUsage usage = memUsageSupplier.get();
        sink.addSize(MEMORY_POOL_VAR_USED, usage.getUsed());
        sink.addSize(MEMORY_POOL_VAR_COMMITTED, usage.getCommitted());
        sink.addSize(MEMORY_POOL_VAR_INIT, usage.getInit());
        sink.addSize(MEMORY_POOL_VAR_MAX, usage.getMax());
    }
}
