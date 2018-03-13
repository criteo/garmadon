package com.criteo.hadoop.garmadon.schema.exceptions;

public class DeserializationException extends Exception {
    public DeserializationException(String msg) {
        super(msg);
    }

    public DeserializationException(Exception cause) {
        super(cause);
    }
}
