package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;

import java.util.HashMap;
import java.util.Map;

import static com.criteo.hadoop.garmadon.heuristics.HeuristicHelper.createCounterHeuristic;

public class G1GC implements GCStatsHeuristic {
    private final HeuristicsResultDB heuristicsResultDB;
    private final Map<String, Map<String, FullGCCounters>> appFullGC = new HashMap<>();

    public G1GC(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(Long timestamp, String applicationId, String attemptId, String containerId, JVMStatisticsEventsProtos.GCStatisticsData gcStats) {
        if (GCHelper.gcKind(gcStats.getCollectorName()) != GCHelper.GCKind.G1) return;
        GCHelper.GCGenKind gcGenKind = GCHelper.gcGenKind(gcStats.getCollectorName());
        if (gcGenKind == GCHelper.GCGenKind.MAJOR) {
            Map<String, FullGCCounters> containerFullGC = appFullGC.computeIfAbsent(HeuristicHelper.getAppAttemptId(applicationId, attemptId),
                s -> new HashMap<>());
            FullGCCounters details = containerFullGC.computeIfAbsent(containerId, s -> new FullGCCounters(timestamp, gcStats.getPauseTime()));
            details.count++;
            if (details.count > 1) details.pauseTime += gcStats.getPauseTime();
            details.severity = HeuristicsResultDB.Severity.SEVERE;
        }
    }

    @Override
    public void onContainerCompleted(String applicationId, String attemptId, String containerId) {

    }

    @Override
    public void onAppCompleted(String applicationId, String attemptId) {
        createCounterHeuristic(applicationId, attemptId, appFullGC, heuristicsResultDB, G1GC.class, counter -> {
            if (counter.count == 1) {
                return "Timestamp: " + HeuristicResult.formatTimestamp(counter.timestamp) + ", pauseTime: " + counter.pauseTime + "ms";
            } else {
                return "Count: " + counter.count + ", Cumulative PauseTime: " + counter.pauseTime + "ms";
            }
        });
    }

    @Override
    public String getHelp() {
        return HeuristicHelper.loadHelpFile("G1GC");
    }

    private static class FullGCCounters extends BaseCounter {
        private int count;
        private long timestamp;
        private long pauseTime;

        FullGCCounters(long timestamp, long pauseTime) {
            this.timestamp = timestamp;
            this.pauseTime = pauseTime;
        }
    }
}
