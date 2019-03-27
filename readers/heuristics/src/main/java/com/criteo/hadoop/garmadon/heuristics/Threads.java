package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;

import java.util.HashMap;
import java.util.Map;

public class Threads implements JVMStatsHeuristic {
    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, Map<String, ThreadCounters>> appCounters = new HashMap<>();

    public Threads(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(Long timestamp, String applicationId, String attemptId, String containerId, JVMStatisticsEventsProtos.JVMStatisticsData jvmStats) {
        for (JVMStatisticsEventsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            if ("threads".equals(section.getName())) {
                Map<String, ThreadCounters> containerCounters = appCounters.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId),
                    s -> new HashMap<>());
                ThreadCounters threadCounters = containerCounters.computeIfAbsent(containerId, s -> new ThreadCounters());
                for (JVMStatisticsEventsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    if ("count".equals(property.getName())) {
                        int current = Integer.parseInt(property.getValue());
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
    public void onContainerCompleted(String applicationId, String attemptId, String containerId) {
        Map<String, ThreadCounters> containerCounters = appCounters.get(HeuristicHelper.getAppAttemptId(applicationId, attemptId));
        if (containerCounters == null) return;
        ThreadCounters threadCounters = containerCounters.get(containerId);
        if (threadCounters == null) return;
        int severity = HeuristicsResultDB.Severity.NONE;
        int ratio = threadCounters.maxCount * 100 / threadCounters.total;
        if (ratio <= 10) { // maxcount=10 total>100 (+90 thread created/destroyed)
            severity = HeuristicsResultDB.Severity.LOW;
        }
        if (ratio <= 0) { // maxcount=10 total>1000 (+900 thread created/destroyed)
            severity = HeuristicsResultDB.Severity.MODERATE;
        }
        if (severity == HeuristicsResultDB.Severity.NONE) {
            containerCounters.remove(containerId);
            return;
        }
        threadCounters.ratio = ratio;
        threadCounters.severity = severity;
    }

    @Override
    public void onAppCompleted(String applicationId, String attemptId) {
        HeuristicHelper.createCounterHeuristic(applicationId, attemptId, appCounters, heuristicsResultDB, Threads.class,
            counter -> "Max count threads: " + counter.maxCount + ", Total threads: " + counter.total);
    }

    @Override
    public String getHelp() {
        return HeuristicHelper.loadHelpFile("Threads");
    }

    private static class ThreadCounters extends BaseCounter {
        private int maxCount;
        private int total;
        private int ratio;
    }
}
