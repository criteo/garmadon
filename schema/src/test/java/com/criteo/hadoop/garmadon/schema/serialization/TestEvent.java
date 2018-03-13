package com.criteo.hadoop.garmadon.schema.serialization;


public class TestEvent {
    public byte[] bb = new byte[]{1, 2};

    public byte[] serialize() {
        return bb;
    }

    public static TestEvent parseFrom(java.io.InputStream input){
        return new TestEvent();
    }
}
