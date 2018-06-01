package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

public interface JVMStatsHeuristic {
    void process(String applicationId, String containerId, JVMStatisticsProtos.JVMStatisticsData jvmStats);
    void onCompleted(String applicationId, String containerId);
}
