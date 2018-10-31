package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class GCHelper {
    public enum GCGenKind {
        MINOR,
        MAJOR
    }

    public enum GCKind {
        SERIAL,
        PARALLEL,
        CMS,
        G1
    }

    public static GCGenKind gcGenKind(String gcName) {
        switch (gcName) {
            case "Copy": return GCGenKind.MINOR;
            case "PS Scavenge": return GCGenKind.MINOR;
            case "ParNew": return GCGenKind.MINOR;
            case "G1 Young Generation": return GCGenKind.MINOR;

            case "MarkSweepCompact": return GCGenKind.MAJOR;
            case "PS MarkSweep": return GCGenKind.MAJOR;
            case "ConcurrentMarkSweep": return GCGenKind.MAJOR;
            case "G1 Old Generation": return GCGenKind.MAJOR;
        }
        throw new IllegalArgumentException("Unknown gc name: " + gcName);
    }

    public static GCKind gcKind(String gcName) {
        switch (gcName) {
            case "Copy": return GCKind.SERIAL;
            case "PS Scavenge": return GCKind.PARALLEL;
            case "ParNew": return GCKind.CMS;
            case "G1 Young Generation": return GCKind.G1;

            case "MarkSweepCompact": return GCKind.SERIAL;
            case "PS MarkSweep": return GCKind.PARALLEL;
            case "ConcurrentMarkSweep": return GCKind.CMS;
            case "G1 Old Generation": return GCKind.G1;
        }
        throw new IllegalArgumentException("Unknown gc name: " + gcName);
    }

    public static HeuristicResult addGCDetails(HeuristicResult result, Long timestamp, JVMStatisticsProtos.GCStatisticsData gcStats) {
        result.addDetail("Timestamp", String.valueOf(timestamp), HeuristicResult.formatTimestamp(timestamp));
        result.addDetail("Collector", gcStats.getCollectorName());
        result.addDetail("Pause", String.valueOf(gcStats.getPauseTime()));
        return result;
    }
}
