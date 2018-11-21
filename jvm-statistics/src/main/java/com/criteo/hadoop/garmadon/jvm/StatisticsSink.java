package com.criteo.hadoop.garmadon.jvm;

/**
 * Sink used to dump statistics
 */
public interface StatisticsSink<T> {
    StatisticsSink<T> beginSection(String name);

    StatisticsSink<T> endSection();

    StatisticsSink<T> addDuration(String property, long timeInMilli);

    StatisticsSink<T> addSize(String property, long sizeInBytes);

    StatisticsSink<T> addPercentage(String property, int percent);

    StatisticsSink<T> add(String property, String value);

    StatisticsSink<T> add(String property, int value);

    StatisticsSink<T> add(String property, long value);

    T flush();
}
