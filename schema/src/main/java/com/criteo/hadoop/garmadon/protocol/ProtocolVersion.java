package com.criteo.hadoop.garmadon.protocol;

import java.util.Arrays;
import java.util.List;

public class ProtocolVersion {
    public static final byte VERSION = 2;
    public static final byte[] GREETINGS = new byte[]{0, 0, 'V', VERSION};
    private static final List<Byte> AUTHORIZED_RELEASE = Arrays.asList(VERSION);

    protected ProtocolVersion() {
        throw new UnsupportedOperationException();
    }

    public static void checkVersion(byte[] greetings) throws InvalidFrameException, InvalidProtocolVersionException {
        if (greetings[0] != 0) throw new InvalidFrameException("cannot extract version from server greetings");
        if (greetings[1] != 0) throw new InvalidFrameException("cannot extract version from server greetings");
        if (greetings[2] != 'V') throw new InvalidFrameException("cannot extract version from server greetings");
        if (!AUTHORIZED_RELEASE.contains(greetings[3])) {
            throw new InvalidProtocolVersionException(VERSION, greetings[3]);
        }
    }

    public static class InvalidProtocolVersionException extends Exception {
        InvalidProtocolVersionException(byte expected, byte actual) {
            super("expected server protocol version " + expected + " found " + actual);
        }
    }

    public static class InvalidFrameException extends Exception {
        InvalidFrameException(String msg) {
            super(msg);
        }
    }
}
