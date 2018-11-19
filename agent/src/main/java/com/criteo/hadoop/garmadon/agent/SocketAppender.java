package com.criteo.hadoop.garmadon.agent;

import com.criteo.hadoop.garmadon.protocol.ProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Blocking implementation of a socket appender
 * <p>
 * Caution: this class is not thread safe so only one thread should use it !
 */
public class SocketAppender {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketAppender.class);

    private Connection connection;

    public SocketAppender(String host, int port) {
        this.connection = new Connection(host, port);
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

    public static class Connection {

        private static final int CONNECTION_TIMEOUT = 10000;
        private static final int READ_TIMEOUT = 10000;
        private final String host;
        private final int port;

        private Socket socket;

        private boolean connectionEstablished = false;

        private OutputStream out;
        private InputStream in;

        public Connection(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public void write(byte[] bytes) throws IOException {
            out.write(bytes);
        }

        public int read(byte[] buf) throws IOException {
            return in.read(buf);
        }

        public void establishConnection() {
            for (; ; ) {
                try {
                    LOGGER.debug("try connecting to {}:{}", host, port);

                    socket = new Socket();
                    socket.connect(new InetSocketAddress(host, port), CONNECTION_TIMEOUT);
                    socket.setSoTimeout(READ_TIMEOUT);

                    out = socket.getOutputStream();
                    in = socket.getInputStream();

                    makeHandshake();

                    LOGGER.debug("connection established");
                    connectionEstablished = true;
                    return;
                } catch (IOException | ProtocolVersion.InvalidFrameException | ProtocolVersion.InvalidProtocolVersionException exception) {
                    close();
                    waitBeforeRetry();
                }
            }
        }

        public boolean isConnected() {
            return connectionEstablished;
        }

        private void makeHandshake() throws IOException, ProtocolVersion.InvalidFrameException, ProtocolVersion.InvalidProtocolVersionException {
            //send greetings
            write(ProtocolVersion.GREETINGS);

            //receive ack
            byte[] buf = new byte[ProtocolVersion.GREETINGS.length];
            read(buf);

            ProtocolVersion.checkVersion(buf);
        }

        private void waitBeforeRetry() {
            LOGGER.debug("cannot connect to {}:{}", host, port);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void close() {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
            connectionEstablished = false;
        }

    }
}
