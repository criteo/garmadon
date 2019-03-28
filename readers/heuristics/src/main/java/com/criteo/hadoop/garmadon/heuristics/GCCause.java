package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;

import java.util.HashMap;
import java.util.Map;

import static com.criteo.hadoop.garmadon.heuristics.HeuristicHelper.MAX_CONTAINERS_PER_HEURISTIC;

public class GCCause implements GCStatsHeuristic {
    static final String METADATA_THRESHOLD = "Metadata GC Threshold";
    static final String ERGONOMICS = "Ergonomics";

    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, Map<String, GCCauseStats>> appStats = new HashMap<>();

    public GCCause(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(Long timestamp, String applicationId, String attemptId, String containerId, JVMStatisticsEventsProtos.GCStatisticsData gcStats) {
        if (METADATA_THRESHOLD.equals(gcStats.getCause()) || ERGONOMICS.equals(gcStats.getCause())) {
            Map<String, GCCauseStats> containerStats = appStats.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId),
                s -> new HashMap<>());
            GCCauseStats stats = containerStats.computeIfAbsent(containerId, s -> new GCCauseStats());
            if (METADATA_THRESHOLD.equals(gcStats.getCause())) stats.metadataThreshold++;
            if (ERGONOMICS.equals(gcStats.getCause())) stats.ergonomics++;
        }
    }

    @Override
    public void onContainerCompleted(String applicationId, String attemptId, String containerId) {

    }

    @Override
    public void onAppCompleted(String applicationId, String attemptId) {
        Map<String, GCCauseStats> containerStats = appStats.remove(HeuristicHelper.getAppAttemptId(applicationId, attemptId));
        if (containerStats == null) return;
        HeuristicResult result = new HeuristicResult(applicationId, attemptId, GCCause.class, HeuristicsResultDB.Severity.MODERATE,
            HeuristicsResultDB.Severity.MODERATE);
        if (containerStats.size() <= MAX_CONTAINERS_PER_HEURISTIC) {
            containerStats.forEach((key, value) -> result.addDetail(key, METADATA_THRESHOLD + ": " + value.metadataThreshold + ", " + ERGONOMICS
                + ": " + value.ergonomics));
        } else {
            int metadataThresholdCount = containerStats.values().stream().mapToInt(stats -> stats.metadataThreshold).sum();
            int ergonomicsCount = containerStats.values().stream().mapToInt(stats -> stats.ergonomics).sum();
            result.addDetail(METADATA_THRESHOLD, String.valueOf(metadataThresholdCount));
            result.addDetail(ERGONOMICS, String.valueOf(ergonomicsCount));
        }
        heuristicsResultDB.createHeuristicResult(result);
    }

    @Override
    public String getHelp() {
        return HeuristicHelper.loadHelpFile("GCCause");
    }

    private static class GCCauseStats {
        private int metadataThreshold;
        private int ergonomics;
    }
}
