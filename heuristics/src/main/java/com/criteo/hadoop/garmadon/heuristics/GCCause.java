package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCCause implements GCStatsHeuristic {
    private static final Logger LOGGER = LoggerFactory.getLogger(Heuristics.class);

    private final HeuristicsResultDB heuristicsResultDB;

    public GCCause(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void process(String applicationId, String containerId, JVMStatisticsProtos.GCStatisticsData gcStats) {
        if ("Metadata GC Threshold".equals(gcStats.getCause()) || "Ergonomics".equals(gcStats.getCause())) {
            //LOGGER.info("Metadata Threshold found!");
            HeuristicResult result = new HeuristicResult(applicationId, containerId, GCCause.class, HeuristicsResultDB.Severity.MODERATE, HeuristicsResultDB.Severity.MODERATE);
            GCHelper.addGCDetails(result, gcStats);
            result.addDetail("Cause", gcStats.getCause());
            heuristicsResultDB.createHeuristicResult(result);
        }
    }

    @Override
    public void onCompleted(String applicationId, String containerId) {

    }
}
