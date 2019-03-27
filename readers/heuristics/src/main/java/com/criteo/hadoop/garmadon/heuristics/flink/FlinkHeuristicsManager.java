package com.criteo.hadoop.garmadon.heuristics.flink;

import com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FlinkHeuristicsManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkHeuristicsManager.class);

    private final List<FlinkHeuristic> heuristics = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public FlinkHeuristicsManager(List<FlinkHeuristic> flinkHeuristics) {
        heuristics.addAll(flinkHeuristics);

        // Schedule an export every hour
        scheduler.scheduleWithFixedDelay(() -> exportHeuristicsResults(), 1, 1, TimeUnit.HOURS);
    }

    private void exportHeuristicsResults() {
        heuristics.forEach(heuristic -> {
            try {
                heuristic.exportHeuristicsResults();

            } catch (RuntimeException e) {
                LOGGER.error("Failed to exportHeuristicsResults for [{}]", heuristic, e);
            }
        });
    }

    public void processFlinkJobManagerEvent(GarmadonMessage msg) {
        String applicationId = msg.getHeader().getApplicationId();
        Long timestamp = msg.getTimestamp();
        FlinkEventProtos.JobManagerEvent event = (FlinkEventProtos.JobManagerEvent) msg.getBody();
        heuristics.forEach(h -> h.process(timestamp, applicationId, event));
    }

    public void processFlinkJobEvent(GarmadonMessage msg) {
        String applicationId = msg.getHeader().getApplicationId();
        Long timestamp = msg.getTimestamp();
        FlinkEventProtos.JobEvent event = (FlinkEventProtos.JobEvent) msg.getBody();
        heuristics.forEach(h -> h.process(timestamp, applicationId, event));
    }

    public List<FlinkHeuristic> getHeuristics() {
        return new ArrayList<>(heuristics);
    }

    public void onAppCompleted(String applicationId, String attemptId) {
        heuristics.forEach(h -> h.onAppCompleted(applicationId, attemptId));
    }
}
