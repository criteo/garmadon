package com.criteo.hadoop.garmadon.hdfs.writer;

import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.function.Function;

public class DelayedDailyPathComputer implements Function<Temporal, String> {
    private TemporalAmount graceDelay;

    /**
     * @param temporalDelay    How much time after a given day is considered to not be part of that day anymore.
     */
    public DelayedDailyPathComputer(TemporalAmount temporalDelay) {
        this.graceDelay = temporalDelay;
    }

    @Override
    public String apply(Temporal localDateTime) {
        Temporal actualDayBucket = localDateTime.minus(graceDelay);

        return DateTimeFormatter.ofPattern("YYYY-MM-dd").format(actualDayBucket);
    }
}
