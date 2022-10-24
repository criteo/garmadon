package com.criteo.hadoop.garmadon.hdfs;

import com.google.protobuf.Message;

import java.util.function.Function;

public class GarmadonEventDescriptor {
    private final String path;
    private final Class<? extends Message> clazz;
    private final Message.Builder emptyMessageBuilder;
    private final Function<Message, Message> bodyTransformer;

    GarmadonEventDescriptor(String path,
                            Class<? extends Message> clazz,
                            Message.Builder emptyMessageBuilder,
                            Function<Message, Message> bodyTransformer
                            ) {
        this.path = path;
        this.clazz = clazz;
        this.emptyMessageBuilder = emptyMessageBuilder;
        this.bodyTransformer = bodyTransformer;
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

    public Function<Message, Message> getBodyTransformer() {
        return bodyTransformer;
    }
}
