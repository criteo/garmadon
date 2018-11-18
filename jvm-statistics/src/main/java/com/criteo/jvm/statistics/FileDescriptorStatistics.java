package com.criteo.jvm.statistics;

import com.criteo.jvm.AbstractStatistic;
import com.criteo.jvm.StatisticsSink;
import com.sun.management.UnixOperatingSystemMXBean;

class FileDescriptorStatistics extends AbstractStatistic {
    private static final String FILE_DESCRIPTORS_HEADER = "descriptors";
    private static final String FILE_DESCRIPTORS_VAR_OPEN = "open";
    private static final String FILE_DESCRIPTORS_VAR_MAX = "max";

    private final UnixOperatingSystemMXBean unixOs;

    FileDescriptorStatistics(UnixOperatingSystemMXBean unixOs) {
        super(FILE_DESCRIPTORS_HEADER);
        this.unixOs = unixOs;
    }

    @Override
    protected void innerCollect(StatisticsSink sink) {
        sink.add(FILE_DESCRIPTORS_VAR_OPEN, unixOs.getOpenFileDescriptorCount());
        sink.add(FILE_DESCRIPTORS_VAR_MAX, unixOs.getMaxFileDescriptorCount());
    }
}
