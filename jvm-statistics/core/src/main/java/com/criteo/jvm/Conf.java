package com.criteo.jvm;

import java.time.Duration;
import java.util.function.Consumer;

public class Conf<JVMSTATS, GCSTATS, MACHINESTATS> {
    public static final String DEFAULT_SINK_TYPE = "log";

    private Duration interval = Duration.ofMinutes(1);
    private String sinkType = DEFAULT_SINK_TYPE;
    private Consumer<JVMSTATS> logJVMStats;
    private Consumer<GCSTATS> logGcStats;
    private Consumer<MACHINESTATS> logMachineStats;
    private boolean OSStatsInJVMStats;

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

    public Consumer<JVMSTATS> getLogJVMStats() {
        return logJVMStats;
    }

    public void setLogJVMStats(Consumer<JVMSTATS> logJVMStats) {
        this.logJVMStats = logJVMStats;
    }

    public boolean isGcStatsEnabled() {
        return logGcStats != null;
    }

    public Consumer<GCSTATS> getLogGcStats() {
        return logGcStats;
    }

    public void setLogGcStats(Consumer<GCSTATS> logGCStats) {
        this.logGcStats = logGCStats;
    }

    public boolean isMachineStatsEnabled() {
        return logMachineStats != null;
    }

    public Consumer<MACHINESTATS> getLogMachineStats() {
        return logMachineStats;
    }

    public void setLogMachineStats(Consumer<MACHINESTATS> logMachineStats) {
        this.logMachineStats = logMachineStats;
    }

    public boolean isOSStatsInJVMStats() {
        return OSStatsInJVMStats;
    }

    public void setOSStatsInJVMStats(boolean OSStatsInJVMStats) {
        this.OSStatsInJVMStats = OSStatsInJVMStats;
    }
}
