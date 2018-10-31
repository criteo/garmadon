package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

public interface JVMStatsHeuristic extends Heuristic {
    void process(Long timestamp, String applicationId, String attemptId, String containerId, JVMStatisticsProtos.JVMStatisticsData jvmStats);
}
