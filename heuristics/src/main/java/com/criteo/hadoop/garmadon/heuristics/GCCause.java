package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

import java.util.HashMap;
import java.util.Map;

import static com.criteo.hadoop.garmadon.heuristics.HeuristicHelper.MAX_CONTAINERS_PER_HEURISTIC;

public class GCCause implements GCStatsHeuristic {
    static final String METADATA_THRESHOLD = "Metadata GC Threshold";
    static final String ERGONOMICS = "Ergonomics";

    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, Map<String, GCCauseStats>> appStats  = new HashMap<>();

    public GCCause(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(String applicationId, String containerId, JVMStatisticsProtos.GCStatisticsData gcStats) {
        if (METADATA_THRESHOLD.equals(gcStats.getCause()) || ERGONOMICS.equals(gcStats.getCause())) {
            Map<String, GCCauseStats> containerStats = appStats.computeIfAbsent(applicationId, s -> new HashMap<>());
            GCCauseStats stats = containerStats.computeIfAbsent(containerId, s -> new GCCauseStats());
            if (METADATA_THRESHOLD.equals(gcStats.getCause()))
                stats.metadataThreshold++;
            if (ERGONOMICS.equals(gcStats.getCause()))
                stats.ergonomics++;
        }
    }

    @Override
    public void onContainerCompleted(String applicationId, String containerId) {

    }

    @Override
    public void onAppCompleted(String applicationId) {
        Map<String, GCCauseStats> containerStats = appStats.remove(applicationId);
        if (containerStats == null)
            return;
        HeuristicResult result = new HeuristicResult(applicationId, GCCause.class, HeuristicsResultDB.Severity.MODERATE, HeuristicsResultDB.Severity.MODERATE);
        if (containerStats.size() <= MAX_CONTAINERS_PER_HEURISTIC) {
            containerStats.forEach((key, value) -> result.addDetail(key, METADATA_THRESHOLD + ": " + value.metadataThreshold + ", " + ERGONOMICS + ": " + value.ergonomics));
        } else {
            int metadataThresholdCount = containerStats.values().stream().mapToInt(stats -> stats.metadataThreshold).sum();
            int ergonomicsCount = containerStats.values().stream().mapToInt(stats -> stats.ergonomics).sum();
            result.addDetail(METADATA_THRESHOLD, String.valueOf(metadataThresholdCount));
            result.addDetail(ERGONOMICS, String.valueOf(ergonomicsCount));
        }
        heuristicsResultDB.createHeuristicResult(result);
    }

    private static class GCCauseStats {
        int metadataThreshold;
        int ergonomics;
    }
}
