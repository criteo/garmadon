package com.criteo.hadoop.garmadon.hdfs;

import com.google.protobuf.Message;

public class GarmadonEventDescriptor {
    private final String path;
    private final Class<? extends Message> clazz;
    private final Message.Builder emptyMessageBuilder;

    GarmadonEventDescriptor(String path, Class<? extends Message> clazz, Message.Builder emptyMessageBuilder) {
        this.path = path;
        this.clazz = clazz;
        this.emptyMessageBuilder = emptyMessageBuilder;
    }

    public String getPath() {
        return path;
    }

    Class<? extends Message> getClazz() {
        return clazz;
    }

    Message.Builder getEmptyMessageBuilder() {
        return emptyMessageBuilder;
    }
}
