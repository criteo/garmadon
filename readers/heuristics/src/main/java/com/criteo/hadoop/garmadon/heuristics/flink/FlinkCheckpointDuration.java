package com.criteo.hadoop.garmadon.heuristics.flink;

import com.criteo.hadoop.garmadon.event.proto.FlinkEventProtos;
import com.criteo.hadoop.garmadon.heuristics.BaseCounter;
import com.criteo.hadoop.garmadon.heuristics.HeuristicHelper;
import com.criteo.hadoop.garmadon.heuristics.HeuristicResult;
import com.criteo.hadoop.garmadon.heuristics.HeuristicsResultDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FlinkCheckpointDuration implements FlinkHeuristic {
    protected static final long FIFTEEN_MINUTES_IN_MS = 15 * 60 * 1000;
    protected static final String PROPERTY_NAME = "lastCheckpointDuration";

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkCheckpointDuration.class);

    private final HeuristicsResultDB heuristicsResultDB;

    private final Map<String, Map<String, DurationCounter>> countersByAppId = new HashMap<>();

    public FlinkCheckpointDuration(HeuristicsResultDB heuristicsResultDB) {
        this.heuristicsResultDB = heuristicsResultDB;
    }

    @Override
    public void onAppCompleted(String applicationId, String attemptId) {
        doExportHeuristicsResults(applicationId);
        countersByAppId.remove(applicationId);
    }

    @Override
    public String getHelp() {
        return HeuristicHelper.loadHelpFile("FlinkCheckpointDuration");
    }

    @Override
    public void process(Long timestamp, String applicationId, FlinkEventProtos.JobManagerEvent event) {
        // Not interested by this kind of event
    }

    @Override
    public void process(Long timestamp, String applicationId, FlinkEventProtos.JobEvent event) {
        Map<String, DurationCounter> countersByJobKey = countersByAppId.computeIfAbsent(applicationId, s -> new HashMap<>());

        long duration = event.getMetricsList().stream()
            .filter(property -> PROPERTY_NAME.equals(property.getName()))
            .mapToLong(FlinkEventProtos.Property::getValue)
            .findFirst()
            .orElse(0L);

        String jobKey = event.getJobName();
        DurationCounter counter = countersByJobKey.computeIfAbsent(jobKey, s -> new DurationCounter(jobKey, duration));
        counter.setDuration(duration);
    }

    @Override
    public void exportHeuristicsResults() {
        countersByAppId.keySet().forEach(this::doExportHeuristicsResults);
    }

    private void doExportHeuristicsResults(String applicationId) {
        try {
            Map<String, DurationCounter> countersByJobKey = countersByAppId.get(applicationId);
            if (countersByJobKey == null) {
                return;
            }

            countersByJobKey.values().forEach(counter -> {
                HeuristicResult result = new HeuristicResult(
                    applicationId,
                    counter.jobKey, // AttemptID
                    FlinkCheckpointDuration.class,
                    counter.getSeverity(),
                    counter.getSeverity());

                result.addDetail(PROPERTY_NAME, String.valueOf(counter.duration));
                heuristicsResultDB.createHeuristicResult(result);
            });

            // Force a refresh of all counters
            countersByJobKey.clear();

        } catch (RuntimeException e) {
            LOGGER.error("Failed to exportHeuristicsResults for applicationId[{}]", applicationId, e);
        }
    }

    private class DurationCounter extends BaseCounter {
        private final String jobKey;
        private long duration;

        DurationCounter(String jobKey, long duration) {
            this.jobKey = jobKey;
            this.duration = duration;
        }

        public void setDuration(long duration) {
            // Keep only bigger duration
            if (duration < this.duration) {
                return;
            }

            this.duration = duration;

            // Update severity based on duration or previous value
            if (duration > FIFTEEN_MINUTES_IN_MS || severity == HeuristicsResultDB.Severity.SEVERE) {
                severity = HeuristicsResultDB.Severity.SEVERE;
            }
        }
    }
}
