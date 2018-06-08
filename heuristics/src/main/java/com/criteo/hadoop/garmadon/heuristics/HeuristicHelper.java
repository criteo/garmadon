package com.criteo.hadoop.garmadon.heuristics;

import java.util.Map;
import java.util.function.Function;

public class HeuristicHelper {
    public static final int MAX_CONTAINERS_PER_HEURISTIC = 10;

    public static <T extends BaseCounter> void createCounterHeuristic(String applicationId, Map<String, Map<String, T>> appCounters, HeuristicsResultDB heuristicsResultDB, Class<?> heuristicClass, Function<T, String> getDetailValue) {
        Map<String, T> containerCounters = appCounters.remove(applicationId);
        if (containerCounters == null)
            return;
        int severityMax = containerCounters.values().stream().mapToInt(counters -> counters.severity).max().orElse(HeuristicsResultDB.Severity.NONE);
        if (containerCounters.size() > 0 && containerCounters.size() <= MAX_CONTAINERS_PER_HEURISTIC) {
            HeuristicResult result = new HeuristicResult(applicationId, heuristicClass, severityMax, severityMax);
            containerCounters.forEach((key, value) -> result.addDetail(key, getDetailValue.apply(value)));
            heuristicsResultDB.createHeuristicResult(result);
        } else if (containerCounters.size() > MAX_CONTAINERS_PER_HEURISTIC) {
            HeuristicResult result = new HeuristicResult(applicationId, heuristicClass, severityMax, severityMax);
            result.addDetail("Containers",  String.valueOf(containerCounters.size()));
            heuristicsResultDB.createHeuristicResult(result);
        }
    }

}
