package com.criteo.hadoop.garmadon.schema.events;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;

import java.util.Objects;

public class PathEvent {
    private final String path;
    private final long timestamp;
    private final String type;

    public enum Type {
        INPUT,
        OUTPUT
    }

    public PathEvent(long timestamp, String path, Type type) {
        this.timestamp = timestamp;
        this.path = Objects.requireNonNull(path, "cannot create instance of PathEvent with null path");
        this.type = type.toString();
    }

    public String getPaths() {
        return path;
    }

    public String getType() {
        return type;
    }

    public byte[] serialize() {
        DataAccessEventProtos.PathEvent inputEvent = DataAccessEventProtos.PathEvent
                .newBuilder()
                .setPath(path)
                .setType(type)
                .setTimestamp(timestamp)
                .build();
        return inputEvent.toByteArray();
    }

    //equals and hashcode dont use the time stamp field, for test purposes
    //TODO check if we need the timestamp at this place

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PathEvent pathEvent = (PathEvent) o;
        return  Objects.equals(path, pathEvent.path) &&
                Objects.equals(type, pathEvent.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, timestamp, type);
    }
}
