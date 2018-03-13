package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

import java.util.HashMap;
import java.util.Map;

public class CodeCacheUsage implements JVMStatsHeuristic {
    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, CodeCacheCounters> codeCacheCountersMap = new HashMap<>();

    public CodeCacheUsage(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(String applicationId, String containerId, JVMStatisticsProtos.JVMStatisticsData jvmStats) {
        for (JVMStatisticsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            if ("code".equals(section.getName())) {
                CodeCacheCounters codeCacheCounters = codeCacheCountersMap.computeIfAbsent(containerId, s -> new CodeCacheCounters());
                for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    if ("max".equals(property.getName())) {
                        codeCacheCounters.max = Long.parseLong(property.getValue());
                    }
                    if ("used".equals(property.getName())) {
                        codeCacheCounters.peak = Math.max(Long.parseLong(property.getValue()), codeCacheCounters.peak);
                    }
                }
                long max = codeCacheCounters.max;
                long peak = codeCacheCounters.peak;
                if (max > peak && (max - peak) * 100 / max < 5) {
                    HeuristicResult result = new HeuristicResult(applicationId, containerId, CodeCacheUsage.class, HeuristicsResultDB.Severity.MODERATE, HeuristicsResultDB.Severity.MODERATE);
                    result.addDetail("max", codeCacheCounters.max + "kB");
                    result.addDetail("peak", codeCacheCounters.peak + "kB");
                    heuristicsResultDB.createHeuristicResult(result);
                }
                return;
            }
        }
    }

    @Override
    public void onCompleted(String applicationId, String containerId) {
        codeCacheCountersMap.remove(containerId);
    }

    private static class CodeCacheCounters {
        long peak;
        long max;
    }
}
