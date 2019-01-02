package com.criteo.hadoop.garmadon.heuristics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.function.Function;

public class HeuristicHelper {
    public static final int MAX_CONTAINERS_PER_HEURISTIC = 10;

    private static final Logger LOGGER = LoggerFactory.getLogger(HeuristicHelper.class);

    protected HeuristicHelper() {
        throw new UnsupportedOperationException();
    }

    public static String getAppAttemptId(String applicationId, String attemptId) {
        return applicationId + "#" + attemptId;
    }

    public static <T extends BaseCounter> void createCounterHeuristic(String applicationId, String attemptId, Map<String, Map<String, T>> appCounters,
                                                                      HeuristicsResultDB heuristicsResultDB, Class<?> heuristicClass, Function<T,
            String> getDetailValue) {
        Map<String, T> containerCounters = appCounters.remove(HeuristicHelper.getAppAttemptId(applicationId, attemptId));
        if (containerCounters == null) return;
        int severityMax = containerCounters.values().stream().mapToInt(counters -> counters.severity).max().orElse(HeuristicsResultDB.Severity.NONE);
        if (containerCounters.size() > 0 && containerCounters.size() <= MAX_CONTAINERS_PER_HEURISTIC) {
            HeuristicResult result = new HeuristicResult(applicationId, attemptId, heuristicClass, severityMax, severityMax);
            containerCounters.forEach((key, value) -> result.addDetail(key, getDetailValue.apply(value)));
            heuristicsResultDB.createHeuristicResult(result);
        } else if (containerCounters.size() > MAX_CONTAINERS_PER_HEURISTIC) {
            HeuristicResult result = new HeuristicResult(applicationId, attemptId, heuristicClass, severityMax, severityMax);
            result.addDetail("Containers", String.valueOf(containerCounters.size()));
            heuristicsResultDB.createHeuristicResult(result);
        }
    }

    public static String loadHelpFile(String fileName) {
        String fullFileName = "/helps/" + fileName + ".html";
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Heuristic.class.getResourceAsStream(fullFileName)))) {
            String line;
            while ((line = reader.readLine()) != null) sb.append(line).append("\n");
            return sb.toString();
        } catch (IOException ex) {
            LOGGER.warn("Error loading help for ", fileName, ex);
            throw new RuntimeException(ex);
        }
    }
}
