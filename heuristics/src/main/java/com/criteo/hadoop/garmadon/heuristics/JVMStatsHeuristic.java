package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

public interface JVMStatsHeuristic {
    void process(String applicationId, String containerId, JVMStatisticsProtos.JVMStatisticsData jvmStats);
    void onContainerCompleted(String applicationId, String containerId);
    void onAppCompleted(String applicationId);
}
