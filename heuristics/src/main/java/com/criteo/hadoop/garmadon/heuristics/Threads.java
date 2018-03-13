package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

import java.util.HashMap;
import java.util.Map;

public class Threads implements JVMStatsHeuristic {
    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, ThreadCounters> threadCountersMap = new HashMap<>();

    public Threads(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(String applicationId, String containerId, JVMStatisticsProtos.JVMStatisticsData jvmStats) {
        for (JVMStatisticsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            if ("threads".equals(section.getName())) {
                ThreadCounters threadCounters = this.threadCountersMap.computeIfAbsent(containerId, s -> new ThreadCounters());
                for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    if ("count".equals(property.getName())) {
                        int current = Integer.parseInt(property.getValue());
                        threadCounters.lastCount = current;
                        threadCounters.maxCount = Math.max(current, threadCounters.maxCount);
                    }
                    if ("total".equals(property.getName())) {
                        threadCounters.total = Integer.parseInt(property.getValue());
                    }
                }
                return;
            }
        }
    }

    @Override
    public void onCompleted(String applicationId, String containerId) {
        ThreadCounters threadCounters = this.threadCountersMap.get(containerId);
        if (threadCounters == null)
            return;
        int severity = HeuristicsResultDB.Severity.NONE;
        int ratio = threadCounters.maxCount*100 / threadCounters.total;
        if (ratio <= 10) // maxcount=10 total>100 (+90 thread created/destroyed)
            severity = HeuristicsResultDB.Severity.LOW;
        if (ratio <= 0) // maxcount=10 total>1000 (+900 thread created/destroyed)
            severity = HeuristicsResultDB.Severity.MODERATE;
        HeuristicResult heuristicResult = new HeuristicResult(applicationId, containerId, Threads.class, severity, severity);
        heuristicsResultDB.createHeuristicResult(heuristicResult);
        this.threadCountersMap.remove(containerId);
    }

    private static class ThreadCounters {
        int lastCount;
        int maxCount;
        int total;
        // HdrHistogram?
    }
}
