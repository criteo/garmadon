package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

import java.util.HashMap;
import java.util.Map;

public class Safepoints implements JVMStatsHeuristic {
    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, SafepointsCounters> safepointsCountersMap = new HashMap<>();

    public Safepoints(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(String applicationId, String containerId, JVMStatisticsProtos.JVMStatisticsData jvmStats) {
        for (JVMStatisticsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            if ("safepoints".equals(section.getName())) {
                SafepointsCounters safepointsCounters = safepointsCountersMap.computeIfAbsent(containerId, s -> new SafepointsCounters());
                for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    if ("count".equals(property.getName())) {
                        long lastCount = safepointsCounters.lastCount;
                        long lastTimestamp = safepointsCounters.lastTimestamp;
                        long currentCount = Long.parseLong(property.getValue());
                        long currentTimestamp = jvmStats.getTimestamp();
                        safepointsCounters.lastCount = currentCount;
                        safepointsCounters.lastTimestamp = currentTimestamp;
                        if (currentTimestamp == lastTimestamp) // avoid case of / 0
                            return;
                        if (lastTimestamp == 0)
                            return;
                        if (lastCount == 0)
                            return;
                        // ratio is number of safepoints/s
                        long ratio = (currentCount - lastCount) * 1000 / (currentTimestamp - lastTimestamp);
                        int severity = HeuristicsResultDB.Severity.NONE;
                        if (ratio > 3)
                            severity = HeuristicsResultDB.Severity.LOW;
                        if (ratio > 5)
                            severity = HeuristicsResultDB.Severity.MODERATE;
                        if (ratio > 7)
                            severity = HeuristicsResultDB.Severity.SEVERE;
                        if (ratio > 10)
                            severity = HeuristicsResultDB.Severity.CRITICAL;
                        HeuristicResult result = new HeuristicResult(applicationId, containerId, Safepoints.class, severity, severity);
                        result.addDetail("Last count", String.valueOf(lastCount));
                        result.addDetail("Last timestamp", String.valueOf(lastTimestamp), HeuristicResult.formatTimestamp(lastTimestamp));
                        result.addDetail("Current count", String.valueOf(currentCount));
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
        safepointsCountersMap.remove(containerId);
    }

    private static class SafepointsCounters {
        long lastCount;
        long lastTimestamp;
    }
}
