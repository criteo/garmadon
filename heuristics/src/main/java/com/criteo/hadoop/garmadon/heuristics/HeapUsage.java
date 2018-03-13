package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class HeapUsage implements JVMStatsHeuristic, GCStatsHeuristic {
    private static final Pattern GC_NAME_PATTERN = Pattern.compile("gc\\(([^\\)]+)\\)");

    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, HeapCounters> heapCountersMap = new HashMap<>();


    public HeapUsage(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(String applicationId, String containerId, JVMStatisticsProtos.JVMStatisticsData jvmStats) {
        for (JVMStatisticsProtos.JVMStatisticsData.Section section : jvmStats.getSectionList()) {
            String sectionName = section.getName();
            if ("heap".equals(sectionName)) {
                HeapCounters heapCounters = heapCountersMap.computeIfAbsent(containerId, s -> new HeapCounters());
                for (JVMStatisticsProtos.JVMStatisticsData.Property property : section.getPropertyList()) {
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
                        .map(JVMStatisticsProtos.JVMStatisticsData.Property::getValue)
                        .mapToLong(Long::parseLong)
                        .findFirst().orElse(0);
                HeapCounters heapCounters = heapCountersMap.computeIfAbsent(containerId, s -> new HeapCounters());
                Matcher matcher = GC_NAME_PATTERN.matcher(sectionName);
                if (matcher.find()) {
                    String gcName = matcher.group(1);
                    if (heapCounters.gcKind == null)
                        heapCounters.gcKind = GCHelper.gcKind(gcName);
                    switch (GCHelper.gcGenKind(gcName)) {
                        case MINOR:
                            heapCounters.minorGC = count;
                            break;
                        case MAJOR:
                            heapCounters.majorGC = count;
                            break;
                    }
                }
            }
        }
    }

    @Override
    public void onCompleted(String applicationId, String containerId) {
        HeapCounters heapCounters = heapCountersMap.get(containerId);
        if (heapCounters == null)
            return;
        switch (heapCounters.gcKind) {
            case PARALLEL:
                handleParallelGC(applicationId, containerId, heapCounters);
                break;
            case G1:
                handleG1GC(applicationId, containerId, heapCounters);
                break;
            default:
                break;
        }
        heapCountersMap.remove(containerId);
    }

    private void handleG1GC(String applicationId, String containerId, HeapCounters heapCounters) {
        int severity = HeuristicsResultDB.Severity.NONE;
        if (heapCounters.majorGC == 0 && heapCounters.max > heapCounters.peak) {
            long max = heapCounters.max;
            long peak = heapCounters.peak;
            long ratio = (max - peak) * 100 / max;
            if (ratio > 30)
                severity = HeuristicsResultDB.Severity.LOW;
            if (ratio > 50)
                severity = HeuristicsResultDB.Severity.MODERATE;
            if (ratio > 70)
                severity = HeuristicsResultDB.Severity.SEVERE;
            // TODO add details/reasons
        }
        createResult(applicationId, containerId, severity);
    }

    private void handleParallelGC(String applicationId, String containerId, HeapCounters heapCounters) {
        int severity = HeuristicsResultDB.Severity.NONE;
        if (heapCounters.majorGC == 0 && heapCounters.max > heapCounters.peak) {
            long max = heapCounters.max;
            long peak = heapCounters.peak;
            long ratio = (max - peak) * 100 / max;
            if (ratio > 30)
                severity = HeuristicsResultDB.Severity.LOW;
            if (ratio > 50)
                severity = HeuristicsResultDB.Severity.MODERATE;
            if (ratio > 70)
                severity = HeuristicsResultDB.Severity.SEVERE;
            // TODO add details/reasons
        }
        createResult(applicationId, containerId, severity);
    }

    private void createResult(String applicationId, String containerId, int severity) {
        HeuristicResult heuristicResult = new HeuristicResult(applicationId, containerId, HeapUsage.class, severity, severity);
        heuristicsResultDB.createHeuristicResult(heuristicResult);
    }

    @Override
    public void process(String applicationId, String containerId, JVMStatisticsProtos.GCStatisticsData gcStats) {
        // TODO process GC stats
        // TODO Handle before/after heap used at GC
        // TODO After GC heap used give a good indication of LiveSet!
    }

    private static class HeapCounters {
        long max;
        long peak;
        GCHelper.GCKind gcKind;
        long minorGC;
        long majorGC;
    }
}
