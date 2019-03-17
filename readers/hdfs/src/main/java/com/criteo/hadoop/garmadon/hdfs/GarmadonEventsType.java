package com.criteo.hadoop.garmadon.hdfs;

import com.google.protobuf.Message;

public class GarmadonEventsType {
    private final String path;
    private final Class<? extends Message> clazz;
    private final Message emptyMessage;

    public GarmadonEventsType(String path, Class<? extends Message> clazz, Message emptyMessage) {
        this.path = path;
        this.emptyMessage = emptyMessage;
        this.clazz = clazz;
    }

    public String getPath() {
        return path;
    }

    public Class<? extends Message> getClazz() {
        return clazz;
    }

    public Message getEmptyMessage() {
        return emptyMessage;
    }
}
