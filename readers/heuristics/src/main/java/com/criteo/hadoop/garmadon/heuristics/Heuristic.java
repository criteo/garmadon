package com.criteo.hadoop.garmadon.heuristics;

public interface Heuristic {

    void onContainerCompleted(String applicationId, String attemptId, String containerId);

    void onAppCompleted(String applicationId, String attemptId);

    String getHelp();
}
