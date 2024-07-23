package com.criteo.hadoop.garmadon.agent;

import com.criteo.hadoop.garmadon.protocol.ProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class FixedConnection implements Connection {
    private static final Logger LOGGER = LoggerFactory.getLogger(FixedConnection.class);

    private static final int CONNECTION_TIMEOUT = 10000;
    private static final int READ_TIMEOUT = 10000;
    private final String host;
    private final int port;

    private Socket socket;

    private boolean connectionEstablished = false;

    private OutputStream out;
    private InputStream in;

    public FixedConnection(String host, int port) {
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
            if (establishConnectionOnce()) return;
        }
    }

    boolean establishConnectionOnce() {
        try {
            LOGGER.info("try connecting to {}:{}", host, port);

            socket = new Socket();
            socket.connect(new InetSocketAddress(host, port), CONNECTION_TIMEOUT);
            socket.setSoTimeout(READ_TIMEOUT);

            out = socket.getOutputStream();
            in = socket.getInputStream();

            makeHandshake();

            LOGGER.info("connection established to {}:{}", host, port);
            connectionEstablished = true;
            return true;
        } catch (IOException | ProtocolVersion.InvalidFrameException | ProtocolVersion.InvalidProtocolVersionException exception) {
            close();
            waitBeforeRetry();
        }
        return false;
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
        LOGGER.error("cannot connect to {}:{}", host, port);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
        connectionEstablished = false;
    }

}
