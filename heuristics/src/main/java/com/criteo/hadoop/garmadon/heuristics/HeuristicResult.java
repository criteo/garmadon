package com.criteo.hadoop.garmadon.heuristics;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class HeuristicResult {
    public static class HeuristicResultDetail {
        public final String name;
        public final String value;
        public final String details;

        public HeuristicResultDetail(String name, String value, String details) {
            this.name = name;
            this.value = value;
            this.details = details;
        }
    }

    public final String appId;
    public final String containerId;
    public final Class<?> heuristicClass;
    public final int severity;
    public final int score;
    private final List<HeuristicResultDetail> details = new ArrayList<>();

    public HeuristicResult(String appId, String containerId, Class<?> heuristicClass, int severity, int score) {
        this.appId = appId;
        this.containerId = containerId;
        this.heuristicClass = heuristicClass;
        this.severity = severity;
        this.score = score;
    }

    public void addDetail(HeuristicResultDetail detail) {
        details.add(detail);
    }

    public void addDetail(String name, String value) {
        addDetail(new HeuristicResultDetail(name, value, null));
    }

    public void addDetail(String name, String value, String details) {
        addDetail(new HeuristicResultDetail(name, value, details));
    }

    public int getDetailCount() {
        return details.size();
    }

    public HeuristicResultDetail getDetail(int index) {
        return details.get(index);
    }

    public static String formatTimestamp(long timestamp) {
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(timestamp));
    }
}
