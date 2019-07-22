package com.criteo.hadoop.garmadon.jvm;

public class GcEvent {
    private long startTime;
    private long endTime;

    public GcEvent(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public boolean isTooOld(long maxEndTime) {
        return endTime < maxEndTime;
    }

    public long getPauseDuration() {
        return endTime - startTime;
    }

    public long getPauseDurationSince(long maxEndTime) {
        if (startTime >= maxEndTime) {
            return getPauseDuration();
        } else if (endTime < maxEndTime) {
            return 0;
        }
        return endTime - maxEndTime;
    }
}
