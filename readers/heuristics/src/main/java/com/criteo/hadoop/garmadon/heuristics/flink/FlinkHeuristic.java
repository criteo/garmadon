package com.criteo.hadoop.garmadon.heuristics.flink;

import com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos;
import com.criteo.hadoop.garmadon.heuristics.Heuristic;

public interface FlinkHeuristic extends Heuristic {

    @Override
    default void onContainerCompleted(String applicationId, String attemptId, String containerId) {
        // Not interested by container completed
    }

    void process(Long timestamp, String applicationId, FlinkEventProtos.JobManagerEvent event);

    void process(Long timestamp, String applicationId, FlinkEventProtos.JobEvent event);

    /**
     * Called on a fixed delay to export results into DrElephant of living (and not terminated) Flink job
     */
    void exportHeuristicsResults();

}
