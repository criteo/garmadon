package com.criteo.hadoop.garmadon.heuristics;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;

public interface GCStatsHeuristic extends Heuristic {
    void process(Long timestamp, String applicationId, String attemptId, String containerId, JVMStatisticsEventsProtos.GCStatisticsData gcStats);
}
