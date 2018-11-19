package com.criteo.hadoop.garmadon.jvm;

import java.util.ArrayList;
import java.util.List;

/**
 * StatisticsSink implementation for String representation, mostly used for logging
 */
public class StatisticsLog implements StatisticsSink<String> {
    private final List<String> logs = new ArrayList<>();
    private boolean isFirstSection = true;
    private boolean isFirstProperty = true;
    private int sectionStart;

    @Override
    public StatisticsLog beginSection(String name) {
        sectionStart = logs.size();
        if (isFirstSection) {
            isFirstSection = false;
        } else {
            logs.add(", ");
        }
        logs.add(name);
        logs.add("[");
        isFirstProperty = true;
        return this;
    }

    @Override
    public StatisticsSink addDuration(String property, long timeInMilli) {
        return add(property, timeInMilli);
    }

    @Override
    public StatisticsSink addSize(String property, long sizeInBytes) {
        return add(property, sizeInBytes / 1024);
    }

    @Override
    public StatisticsLog addPercentage(String property, int percent) {
        add("%" + property, String.valueOf(percent));
        return this;
    }

    @Override
    public StatisticsLog add(String property, String value) {
        if (isFirstProperty) {
            isFirstProperty = false;
        } else {
            logs.add(", ");
        }
        logs.add(property);
        logs.add("=");
        logs.add(value);
        return this;
    }

    @Override
    public StatisticsSink<String> add(String property, int value) {
        return add(property, String.valueOf(value));
    }

    @Override
    public StatisticsSink<String> add(String property, long value) {
        return add(property, String.valueOf(value));
    }

    @Override
    public StatisticsLog endSection() {
        if ("[".equals(logs.get(logs.size() - 1))) {
            // empty section, rollback
            while (logs.size() > sectionStart) {
                logs.remove(logs.size() - 1);
            }
        } else {
            logs.add("]");
        }
        return this;
    }

    public StatisticsLog reset() {
        logs.clear();
        isFirstSection = true;
        isFirstProperty = true;
        return this;
    }

    @Override
    public String flush() {
        String str = toString();
        reset();
        return str;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < logs.size(); i++) {
            sb.append(logs.get(i));
        }
        return sb.toString();
    }
}
