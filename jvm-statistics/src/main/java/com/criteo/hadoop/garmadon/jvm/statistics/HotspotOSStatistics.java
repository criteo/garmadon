package com.criteo.hadoop.garmadon.jvm.statistics;

import com.criteo.hadoop.garmadon.jvm.StatisticsSink;
import com.sun.management.OperatingSystemMXBean;

class HotspotOSStatistics extends OSStatistics {
    private static final String OS_VAR_PHYSICAL_FREE = "physicalfree";
    private static final String OS_VAR_PHYSICAL_TOTAL = "physicaltotal";
    private static final String OS_VAR_SWAP_FREE = "swapfree";
    private static final String OS_VAR_SWAP_TOTAL = "swaptotal";
    private static final String OS_VAR_VIRTUAL = "virtual";

    private final OperatingSystemMXBean hsOs;

    HotspotOSStatistics(java.lang.management.OperatingSystemMXBean osx, OperatingSystemMXBean hsOs, int processors) {
        super(osx, processors);
        this.hsOs = hsOs;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        super.innerCollect(sink);
        sink.addSize(OS_VAR_PHYSICAL_FREE, hsOs.getFreePhysicalMemorySize());
        sink.addSize(OS_VAR_PHYSICAL_TOTAL, hsOs.getTotalPhysicalMemorySize());
        sink.addSize(OS_VAR_SWAP_FREE, hsOs.getFreeSwapSpaceSize());
        sink.addSize(OS_VAR_SWAP_TOTAL, hsOs.getTotalSwapSpaceSize());
        sink.addSize(OS_VAR_VIRTUAL, hsOs.getCommittedVirtualMemorySize());
    }
}

