package com.criteo.hadoop.garmadon.forwarder.message;

import java.util.Arrays;
import java.util.Objects;

public class KafkaMessage {

    private final String key;
    private final byte[] value;

    public KafkaMessage(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaMessage that = (KafkaMessage) o;
        return Objects.equals(key, that.key) &&
                Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
