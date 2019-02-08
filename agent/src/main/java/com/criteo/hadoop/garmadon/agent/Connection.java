package com.criteo.hadoop.garmadon.agent;

import java.io.IOException;

public interface Connection {
    void write(byte[] bytes) throws IOException;

    int read(byte[] buf) throws IOException;

    void establishConnection();

    boolean isConnected();

    void close();
}
