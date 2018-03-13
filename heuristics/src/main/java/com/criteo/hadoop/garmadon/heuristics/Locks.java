package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Locks implements JVMStatsHeuristic {
    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, LockCounters> lockCountersMap = new HashMap<>();

    public Locks(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(String applicationId, String containerId, JVMStatisticsProtos.JVMStatisticsData jvmStats) {
        for (JVMStatisticsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            if ("synclocks".equals(section.getName())) {
                LockCounters lockCounters = lockCountersMap.computeIfAbsent(containerId, s -> new LockCounters());
                for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    if ("contendedlockattempts".equals(property.getName())) {
                        long lastContendedCount = lockCounters.lastContendedCount;
                        long lastTimestamp = lockCounters.lastTimestamp;
                        long currentContendedCount = Long.parseLong(property.getValue());
                        long currentTimestamp = jvmStats.getTimestamp();
                        lockCounters.lastContendedCount = currentContendedCount;
                        lockCounters.lastTimestamp = currentTimestamp;
                        if (lastTimestamp == currentTimestamp)
                            return;
                        if (lastTimestamp == 0)
                            return;
                        // ratio is number of contention/s
                        long ratio = (currentContendedCount - lastContendedCount) * 1000 / (currentTimestamp - lastTimestamp);
                        int severity = HeuristicsResultDB.Severity.NONE;
                        if (ratio > 10)
                            severity = HeuristicsResultDB.Severity.LOW;
                        if (ratio > 50)
                            severity = HeuristicsResultDB.Severity.MODERATE;
                        if (ratio > 100)
                            severity = HeuristicsResultDB.Severity.SEVERE;
                        if (ratio > 500)
                            severity = HeuristicsResultDB.Severity.CRITICAL;
                        HeuristicResult result = new HeuristicResult(applicationId, containerId, Locks.class, severity, severity);
                        result.addDetail("Last contended count", String.valueOf(lastContendedCount));
                        result.addDetail("Last timestamp", String.valueOf(lastTimestamp), HeuristicResult.formatTimestamp(lastTimestamp));
                        result.addDetail("Current contended count", String.valueOf(currentContendedCount));
                        result.addDetail("Current timestamp", String.valueOf(currentTimestamp), HeuristicResult.formatTimestamp(currentTimestamp));
                        heuristicsResultDB.createHeuristicResult(result);
                        return;
                    }
                }
            }
        }
    }

    @Override
    public void onCompleted(String applicationId, String containerId) {

    }

    private static class LockCounters {
        long lastContendedCount;
        long lastTimestamp;
    }
}
