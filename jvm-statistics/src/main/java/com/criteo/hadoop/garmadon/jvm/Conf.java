package com.criteo.hadoop.garmadon.jvm;

import java.time.Duration;
import java.util.function.BiConsumer;

public class Conf<JVMSTATS, GCSTATS, MACHINESTATS> {
    public static final String DEFAULT_SINK_TYPE = "log";

    private Duration interval = Duration.ofMinutes(1);
    private String sinkType = DEFAULT_SINK_TYPE;
    private BiConsumer<Long, JVMSTATS> logJVMStats;
    private BiConsumer<Long, GCSTATS> logGcStats;
    private BiConsumer<Long, MACHINESTATS> logMachineStats;
    private boolean osStatsInJvmStats;

    public Duration getInterval() {
        return interval;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setInterval(Duration interval) {
        this.interval = interval;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public boolean isJvmStatsEnabled() {
        return logJVMStats != null;
    }

    public BiConsumer<Long, JVMSTATS> getLogJVMStats() {
        return logJVMStats;
    }

    public void setLogJVMStats(BiConsumer<Long, JVMSTATS> logJVMStats) {
        this.logJVMStats = logJVMStats;
    }

    public boolean isGcStatsEnabled() {
        return logGcStats != null;
    }

    public BiConsumer<Long, GCSTATS> getLogGcStats() {
        return logGcStats;
    }

    public void setLogGcStats(BiConsumer<Long, GCSTATS> logGCStats) {
        this.logGcStats = logGCStats;
    }

    public boolean isMachineStatsEnabled() {
        return logMachineStats != null;
    }

    public BiConsumer<Long, MACHINESTATS> getLogMachineStats() {
        return logMachineStats;
    }

    public void setLogMachineStats(BiConsumer<Long, MACHINESTATS> logMachineStats) {
        this.logMachineStats = logMachineStats;
    }

    public boolean isOsStatsInJvmStats() {
        return osStatsInJvmStats;
    }

    public void setOsStatsInJvmStats(boolean osStatsInJvmStats) {
        this.osStatsInJvmStats = osStatsInJvmStats;
    }
}
