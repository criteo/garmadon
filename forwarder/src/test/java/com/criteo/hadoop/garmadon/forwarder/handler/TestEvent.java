package com.criteo.hadoop.garmadon.forwarder.handler;

import sun.misc.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

class TestEvent {
    byte[] bytes;

    TestEvent(int size) {
        bytes = new byte[size];
        new Random().nextBytes(bytes);
    }

    TestEvent(InputStream is) throws IOException {
        bytes = IOUtils.readFully(is, 0, false);
    }
}
