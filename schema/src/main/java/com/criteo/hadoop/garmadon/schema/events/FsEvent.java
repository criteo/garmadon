package com.criteo.hadoop.garmadon.schema.events;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;

import java.util.Objects;

public class FsEvent {
    private final long timestamp;
    private final String dstPath;
    private final String action;
    private final String uri;
    private String srcPath = null;

    public enum Action {
        READ,
        WRITE,
        RENAME,
        DELETE
    }

    public FsEvent(long timestamp, String dstPath, Action action, String uri) {
        this(timestamp, null, dstPath, action, uri);
    }

    public FsEvent(long timestamp, String srcPath, String dstPath, Action action, String uri) {
        this.timestamp = timestamp;
        this.dstPath = Objects.requireNonNull(dstPath, "cannot create instance of FsEvent with null dstPath");
        this.srcPath = srcPath;
        this.action = action.toString();
        this.uri = uri;
    }

    public byte[] serialize() {
        DataAccessEventProtos.FsEvent.Builder builder = DataAccessEventProtos.FsEvent
                .newBuilder()
                .setTimestamp(timestamp)
                .setAction(this.action)
                .setDstPath(this.dstPath)
                .setUri(this.uri);

        if (srcPath != null) {
            builder.setSrcPath(srcPath);
        }

        return builder.build().toByteArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FsEvent fsEvent = (FsEvent) o;
        return timestamp == fsEvent.timestamp &&
                Objects.equals(dstPath, fsEvent.dstPath) &&
                Objects.equals(action, fsEvent.action) &&
                Objects.equals(uri, fsEvent.uri) &&
                Objects.equals(srcPath, fsEvent.srcPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, dstPath, action, uri, srcPath);
    }
}
