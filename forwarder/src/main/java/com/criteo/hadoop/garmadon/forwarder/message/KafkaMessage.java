package com.criteo.hadoop.garmadon.forwarder.message;

import java.util.Arrays;
import java.util.Objects;

public class KafkaMessage {

    private final byte[] value;

    public KafkaMessage(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    public boolean isBroadCasted() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaMessage that = (KafkaMessage) o;
        return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
