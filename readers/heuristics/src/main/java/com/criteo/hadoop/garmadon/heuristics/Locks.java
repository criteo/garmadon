package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;

import java.util.HashMap;
import java.util.Map;

public class Locks implements JVMStatsHeuristic {
    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, Map<String, LockCounters>> appCounters = new HashMap<>();

    public Locks(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(Long timestamp, String applicationId, String attemptId, String containerId, JVMStatisticsEventsProtos.JVMStatisticsData jvmStats) {
        for (JVMStatisticsEventsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            if ("synclocks".equals(section.getName())) {
                Map<String, LockCounters> containerCounters = appCounters.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId),
                    s -> new HashMap<>());
                LockCounters lockCounters = containerCounters.computeIfAbsent(containerId, s -> new LockCounters());
                for (JVMStatisticsEventsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    if ("contendedlockattempts".equals(property.getName())) {
                        long lastContendedCount = lockCounters.lastContendedCount;
                        long lastTimestamp = lockCounters.lastTimestamp;
                        long currentContendedCount = Long.parseLong(property.getValue());
                        long currentTimestamp = timestamp;
                        lockCounters.lastContendedCount = currentContendedCount;
                        lockCounters.lastTimestamp = currentTimestamp;
                        if (lastTimestamp == currentTimestamp) return;
                        if (lastTimestamp == 0) return;
                        // ratio is number of contention/s
                        long ratio = (currentContendedCount - lastContendedCount) * 1000 / (currentTimestamp - lastTimestamp);
                        int severity = HeuristicsResultDB.Severity.NONE;
                        if (ratio > 10) severity = HeuristicsResultDB.Severity.LOW;
                        if (ratio > 50) severity = HeuristicsResultDB.Severity.MODERATE;
                        if (ratio > 100) severity = HeuristicsResultDB.Severity.SEVERE;
                        if (ratio > 500) severity = HeuristicsResultDB.Severity.CRITICAL;
                        lockCounters.ratio = Math.max(ratio, lockCounters.ratio);
                        lockCounters.severity = Math.max(severity, lockCounters.severity);
                        return;
                    }
                }
            }
        }
    }

    @Override
    public void onContainerCompleted(String applicationId, String attemptId, String containerId) {
        Map<String, LockCounters> containerCounters = appCounters.get(HeuristicHelper.getAppAttemptId(applicationId, attemptId));
        if (containerCounters == null) return;
        LockCounters lockCounters = containerCounters.get(containerId);
        if (lockCounters == null) return;
        if (lockCounters.severity == HeuristicsResultDB.Severity.NONE) containerCounters.remove(containerId);
    }

    @Override
    public void onAppCompleted(String applicationId, String attemptId) {
        HeuristicHelper.createCounterHeuristic(applicationId, attemptId, appCounters, heuristicsResultDB, Locks.class,
            counter -> "Max contention/s: " + counter.ratio);
    }

    @Override
    public String getHelp() {
        return HeuristicHelper.loadHelpFile("Locks");
    }

    private static class LockCounters extends BaseCounter {
        private long lastContendedCount;
        private long lastTimestamp;
        private long ratio;
    }
}
