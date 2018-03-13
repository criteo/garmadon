package com.criteo.hadoop.garmadon.schema.events;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;

import java.util.Objects;

public class MetricEvent {
    private final long timestamp;
    private DataAccessEventProtos.MetricEvent.Builder builder;

    public MetricEvent(long timestamp) {
        this.timestamp = timestamp;
        builder = DataAccessEventProtos.MetricEvent
                .newBuilder()
                .setTimestamp(timestamp);
    }

    public void addMetric(DataAccessEventProtos.Metric metric) {
        builder.addMetric(metric);
    }

    public byte[] serialize() {
        return builder.build().toByteArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricEvent)) return false;
        MetricEvent that = (MetricEvent) o;
        return timestamp == that.timestamp &&
                Objects.equals(builder, that.builder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, builder);
    }
}
