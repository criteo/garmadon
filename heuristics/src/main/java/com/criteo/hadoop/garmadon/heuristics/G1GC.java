package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

public class G1GC implements GCStatsHeuristic {
    private final HeuristicsResultDB heuristicsResultDB;

    public G1GC(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(String applicationId, String containerId, JVMStatisticsProtos.GCStatisticsData gcStats) {
        if (GCHelper.gcKind(gcStats.getCollectorName()) != GCHelper.GCKind.G1)
            return;
        GCHelper.GCGenKind gcGenKind = GCHelper.gcGenKind(gcStats.getCollectorName());
        if (gcGenKind == GCHelper.GCGenKind.MAJOR) {
            int severity = HeuristicsResultDB.Severity.SEVERE;
            HeuristicResult result = new HeuristicResult(applicationId, containerId, G1GC.class, severity, severity);
            GCHelper.addGCDetails(result, gcStats);
            heuristicsResultDB.createHeuristicResult(result);
        }
    }

    @Override
    public void onCompleted(String applicationId, String containerId) {

    }
}
