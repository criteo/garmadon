package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

import java.util.HashMap;
import java.util.Map;

public class CodeCacheUsage implements JVMStatsHeuristic {
    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, Map<String, CodeCacheCounters>> appCounters = new HashMap<>();

    public CodeCacheUsage(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(String applicationId, String attemptId, String containerId, JVMStatisticsProtos.JVMStatisticsData jvmStats) {
        for (JVMStatisticsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            if ("code".equals(section.getName())) {
                Map<String, CodeCacheCounters> containerCounters = appCounters.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId), s -> new HashMap<>());
                CodeCacheCounters codeCacheCounters = containerCounters.computeIfAbsent(containerId, s -> new CodeCacheCounters());
                for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    if ("max".equals(property.getName())) {
                        codeCacheCounters.max = Long.parseLong(property.getValue());
                    }
                    if ("used".equals(property.getName())) {
                        codeCacheCounters.peak = Math.max(Long.parseLong(property.getValue()), codeCacheCounters.peak);
                    }
                }
                return;
            }
        }
    }

    @Override
    public void onContainerCompleted(String applicationId, String attemptId, String containerId) {
        Map<String, CodeCacheCounters> containerCounters = appCounters.get(HeuristicHelper.getAppAttemptId(applicationId, attemptId));
        if (containerCounters == null)
            return;
        CodeCacheCounters counters = containerCounters.get(containerId);
        if (counters == null)
            return;
        long max = counters.max;
        long peak = counters.peak;
        counters.severity = HeuristicsResultDB.Severity.MODERATE;
        if (max > peak && (max - peak) * 100 / max >= 5) {
            // no issue with this container, CodeCache is less than 95%
            containerCounters.remove(containerId);
        }
    }

    @Override
    public void onAppCompleted(String applicationId, String attemptId) {
        HeuristicHelper.createCounterHeuristic(applicationId, attemptId, appCounters, heuristicsResultDB, CodeCacheUsage.class,
                counter -> "max: " + counter.max + "kB, peak: " + counter.peak + "kB");
    }

    private static class CodeCacheCounters extends BaseCounter {
        long peak;
        long max;
    }
}
