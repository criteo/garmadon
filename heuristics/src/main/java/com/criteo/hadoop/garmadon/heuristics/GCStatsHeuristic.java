package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

public interface GCStatsHeuristic {
    void process(String applicationId, String attemptId, String containerId, JVMStatisticsProtos.GCStatisticsData gcStats);
    void onContainerCompleted(String applicationId, String attemptId, String containerId);
    void onAppCompleted(String applicationId, String attemptId);
}
