package com.criteo.hadoop.garmadon.schema.events;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;

import java.util.Objects;

public class StateEvent {
    private final long timestamp;
    private final String state;

    public enum State {
        END
    }

    public StateEvent(long timestamp, String state) {
        this.timestamp = timestamp;
        this.state = state;
    }

    public byte[] serialize() {
        return DataAccessEventProtos.StateEvent
                .newBuilder()
                .setTimestamp(timestamp)
                .setState(state)
                .build().toByteArray();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getState() {
        return state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StateEvent)) return false;
        StateEvent that = (StateEvent) o;
        return timestamp == that.timestamp &&
                Objects.equals(state, that.state);
    }

    @Override
    public int hashCode() {

        return Objects.hash(timestamp, state);
    }
}
