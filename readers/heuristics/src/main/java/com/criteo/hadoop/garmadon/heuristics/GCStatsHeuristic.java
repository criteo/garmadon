package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

public interface GCStatsHeuristic extends Heuristic {
    void process(String applicationId, String attemptId, String containerId, JVMStatisticsProtos.GCStatisticsData gcStats);
}
