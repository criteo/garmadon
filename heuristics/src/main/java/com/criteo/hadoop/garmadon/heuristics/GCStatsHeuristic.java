package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.jvm.JVMStatisticsProtos;

public interface GCStatsHeuristic {
    String CATEGORY_NAME = "gc";
    void process(String applicationId, String containerId, JVMStatisticsProtos.GCStatisticsData gcStats);
    void onCompleted(String applicationId, String containerId);
}
