package com.criteo.hadoop.garmadon.protocol;

/**
 * Provide constants for garmadon protocol
 */
public class ProtocolConstants {

    public static final int GREETING_SIZE = 4;

    public static final int FRAME_DELIMITER_SIZE = 20;
    public static final int HEADER_SIZE_INDEX = 12;
    public static final int BODY_SIZE_INDEX = 16;

    protected ProtocolConstants() {
        throw new UnsupportedOperationException();
    }
}
