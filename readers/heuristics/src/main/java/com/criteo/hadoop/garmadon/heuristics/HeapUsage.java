package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HeapUsage implements JVMStatsHeuristic, GCStatsHeuristic {
    private static final Pattern GC_NAME_PATTERN = Pattern.compile("gc\\(([^\\)]+)\\)");

    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, Map<String, HeapCounters>> appCounters = new HashMap<>();


    public HeapUsage(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(Long timestamp, String applicationId, String attemptId, String containerId, JVMStatisticsEventsProtos.JVMStatisticsData jvmStats) {
        Map<String, HeapCounters> containerCounters = appCounters.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId),
            s -> new HashMap<>());
        HeapCounters heapCounters = containerCounters.computeIfAbsent(containerId, s -> new HeapCounters());
        for (JVMStatisticsEventsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            String sectionName = section.getName();
            if ("heap".equals(sectionName)) {
                for (JVMStatisticsEventsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
                    if ("max".equals(property.getName())) {
                        heapCounters.max = Long.parseLong(property.getValue());
                    }
                    if ("used".equals(property.getName())) {
                        heapCounters.peak = Math.max(Long.parseLong(property.getValue()), heapCounters.peak);
                    }
                }
            }
            if (sectionName.startsWith("gc(")) {
                long count = section.getPropertyList().stream()
                    .filter(property -> property.getName().equals("count"))
                    .map(JVMStatisticsEventsProtos.JVMStatisticsData.Property::getValue)
                    .mapToLong(Long::parseLong)
                    .findFirst().orElse(0);
                Matcher matcher = GC_NAME_PATTERN.matcher(sectionName);
                if (matcher.find()) {
                    String gcName = matcher.group(1);
                    if (heapCounters.gcKind == null) heapCounters.gcKind = GCHelper.gcKind(gcName);
                    switch (GCHelper.gcGenKind(gcName)) {
                        case MINOR:
                            heapCounters.minorGC = count;
                            break;
                        case MAJOR:
                            heapCounters.majorGC = count;
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    @Override
    public void process(Long timestamp, String applicationId, String attemptId, String containerId, JVMStatisticsEventsProtos.GCStatisticsData gcStats) {
        // TODO process GC stats
        // TODO Handle before/after heap used at GC
        // TODO After GC heap used give a good indication of LiveSet!
    }

    @Override
    public void onContainerCompleted(String applicationId, String attemptId, String containerId) {
        Map<String, HeapCounters> containerCounters = appCounters.get(HeuristicHelper.getAppAttemptId(applicationId, attemptId));
        if (containerCounters == null) return;
        HeapCounters heapCounters = containerCounters.get(containerId);
        if (heapCounters == null) return;
        if (heapCounters.majorGC > 0 || heapCounters.max <= heapCounters.peak) {
            containerCounters.remove(containerId);
            return;
        }
        int severity = HeuristicsResultDB.Severity.NONE;
        long max = heapCounters.max;
        long peak = heapCounters.peak;
        long ratio = (max - peak) * 100 / max;
        if (ratio > 30) severity = HeuristicsResultDB.Severity.LOW;
        if (ratio > 50) severity = HeuristicsResultDB.Severity.MODERATE;
        if (ratio > 70) severity = HeuristicsResultDB.Severity.SEVERE;
        heapCounters.severity = severity;
        heapCounters.ratio = ratio;
    }

    @Override
    public void onAppCompleted(String applicationId, String attemptId) {
        HeuristicHelper.createCounterHeuristic(applicationId, attemptId, appCounters, heuristicsResultDB, HeapUsage.class,
            counter -> "unused memory %: " + counter.ratio);
    }

    @Override
    public String getHelp() {
        return HeuristicHelper.loadHelpFile("HeapUsage");
    }

    private static class HeapCounters extends BaseCounter {
        private long max;
        private long peak;
        private GCHelper.GCKind gcKind;
        private long minorGC;
        private long majorGC;
        private long ratio;
    }
}
