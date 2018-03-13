package com.criteo.hadoop.garmadon.schema.events;

import com.criteo.hadoop.garmadon.event.proto.ContainerEventProtos;

public class ContainerResourceEvent {

    private final long timestamp;
    private final Type type;
    private final long value;
    private final long limit;

    public enum Type {
        MEMORY
    }

    public ContainerResourceEvent(long timestamp, Type type, long value, long limit) {
        this.timestamp = timestamp;
        this.type = type;
        this.value = value;
        this.limit = limit;
    }

    public byte[] serialize() {
        return ContainerEventProtos.ContainerResourceEvent.newBuilder()
                .setTimestamp(timestamp)
                .setType(type.name())
                .setValue(value)
                .setLimit(limit)
                .build()
                .toByteArray();
    }
}
