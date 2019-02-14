package com.criteo.hadoop.garmadon.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Blocking implementation of a socket appender
 * <p>
 * Caution: this class is not thread safe so only one thread should use it !
 */
public class SocketAppender {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketAppender.class);

    private Connection connection;

    public SocketAppender(Connection connection) {
        this.connection = connection;
    }

    public void append(byte[] event) {
        boolean sent = false;
        while (!sent) {

            if (!connection.isConnected()) {
                connection.establishConnection();
            }

            try {
                connection.write(event);
                sent = true;

            } catch (IOException e) {
                e.printStackTrace();
                connection.close();
            }

        }
    }

    public boolean isConnected() {
        return connection.isConnected();
    }

}
