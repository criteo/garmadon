package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;

import java.util.HashMap;
import java.util.Map;

public class Safepoints implements JVMStatsHeuristic {
    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, Map<String, SafepointsCounters>> appCounters = new HashMap<>();

    public Safepoints(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(Long timestamp, String applicationId, String attemptId, String containerId, JVMStatisticsEventsProtos.JVMStatisticsData jvmStats) {
        for (JVMStatisticsEventsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            if ("safepoints".equals(section.getName())) {
                Map<String, SafepointsCounters> containerCounters = appCounters.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId),
                    s -> new HashMap<>());
                SafepointsCounters safepointsCounters = containerCounters.computeIfAbsent(containerId, s -> new SafepointsCounters());
                for (JVMStatisticsEventsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    if ("count".equals(property.getName())) {
                        long lastCount = safepointsCounters.lastCount;
                        long lastTimestamp = safepointsCounters.lastTimestamp;
                        long currentCount = Long.parseLong(property.getValue());
                        long currentTimestamp = timestamp;
                        safepointsCounters.lastCount = currentCount;
                        safepointsCounters.lastTimestamp = currentTimestamp;
                        if (currentTimestamp == lastTimestamp) return; // avoid case of / 0
                        if (lastTimestamp == 0) return;
                        if (lastCount == 0) return;
                        // ratio is number of safepoints/s
                        long ratio = (currentCount - lastCount) * 1000 / (currentTimestamp - lastTimestamp);
                        int severity = HeuristicsResultDB.Severity.NONE;
                        if (ratio > 3) severity = HeuristicsResultDB.Severity.LOW;
                        if (ratio > 5) severity = HeuristicsResultDB.Severity.MODERATE;
                        if (ratio > 7) severity = HeuristicsResultDB.Severity.SEVERE;
                        if (ratio > 10) severity = HeuristicsResultDB.Severity.CRITICAL;
                        safepointsCounters.ratio = Math.max(ratio, safepointsCounters.ratio);
                        safepointsCounters.severity = Math.max(severity, safepointsCounters.severity);
                        return;
                    }
                }
            }
        }
    }

    @Override
    public void onContainerCompleted(String applicationId, String attemptId, String containerId) {
        Map<String, SafepointsCounters> containerCounters = appCounters.get(HeuristicHelper.getAppAttemptId(applicationId, attemptId));
        if (containerCounters == null) return;
        SafepointsCounters safepointsCounters = containerCounters.get(containerId);
        if (safepointsCounters == null) return;
        if (safepointsCounters.severity == HeuristicsResultDB.Severity.NONE) containerCounters.remove(containerId);
    }

    @Override
    public void onAppCompleted(String applicationId, String attemptId) {
        HeuristicHelper.createCounterHeuristic(applicationId, attemptId, appCounters, heuristicsResultDB, Safepoints.class,
            counter -> "Max safepoint/s: " + counter.ratio);
    }

    @Override
    public String getHelp() {
        return HeuristicHelper.loadHelpFile("Safepoints");
    }

    private static class SafepointsCounters extends BaseCounter {
        private long lastCount;
        private long lastTimestamp;
        private long ratio;
    }
}
