package com.criteo.hadoop.garmadon.schema.serialization;

import com.google.protobuf.Message;

public class TestEvent extends
        com.google.protobuf.GeneratedMessageV3 {
    public byte[] bb = new byte[]{1, 2};

    public byte[] serialize() {
        return bb;
    }

    public static TestEvent parseFrom(java.io.InputStream input) {
        return new TestEvent();
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
        return null;
    }

    @Override
    protected Message.Builder newBuilderForType(BuilderParent parent) {
        return null;
    }

    @Override
    public Message.Builder newBuilderForType() {
        return null;
    }

    @Override
    public Message.Builder toBuilder() {
        return null;
    }

    @Override
    public Message getDefaultInstanceForType() {
        return null;
    }
}
