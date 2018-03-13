package com.criteo.hadoop.garmadon.agent.utils;

import com.criteo.hadoop.garmadon.agent.Logger;
import com.criteo.hadoop.garmadon.protocol.ProtocolVersion;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

//single thread server than can handle one connection for test purposes
public class RPCServerMock implements Runnable {

    private static final Logger logger = Logger.getLogger(RPCServerMock.class);

    private final int port;
    private final byte[] greetings;

    private ServerSocket serverSocket;
    private volatile Socket clientSocket;

    //mutable vars
    private boolean started;
    private volatile boolean receivedMsg;

    private Thread serverThread;

    public RPCServerMock(int port, byte[] greetings) {
        this.port = port;
        this.greetings = greetings;
    }

    public synchronized void start() {
        if (!started) {
            logger.info("Starting test server");
            serverThread = new Thread(this);
            serverThread.start();
            started = true;
        }
    }

    public void shutdown() {
        synchronized (this){
            if(started) {
                if (serverSocket != null) {
                    try {
                        serverSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (clientSocket != null) {
                    try {
                        clientSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                started = false;
            }
        }
        if(serverThread != null){
            try {
                serverThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                if(!started) { //server already shutdown
                    logger.debug("server already shutdown");
                    return;
                }
                serverSocket = new ServerSocket(port);
                logger.info("Test server listening on port " + port);
            }
            clientSocket = serverSocket.accept();
            InputStream in = clientSocket.getInputStream();
            OutputStream out = clientSocket.getOutputStream();

            //read/response to greetings
            byte[] buf = new byte[ProtocolVersion.GREETINGS.length];
            in.read(buf);
            if (Arrays.equals(buf, ProtocolVersion.GREETINGS)) {
                out.write(greetings);
                out.flush();
            }

            //wait for a msg
            buf = new byte[100];
            int n = in.read(buf);
            if (n > 0) {
                receivedMsg = true;
            }

        } catch (Exception e) {
            logger.error("Server caught exception", e);
        }
    }

    public void verifyReceiveMsg() {
        if (!receivedMsg) {
            throw new RuntimeException("expected msg but got none");
        }
    }

    public static class Builder {

        private int port;
        private byte[] greetings;

        public Builder(int port){
            this.port = port;
        }

        public static Builder withPort(int port) {
            return new Builder(port);
        }

        public Builder withGreetingsAck(String greetings) {
            this.greetings = greetings.getBytes();
            return this;
        }

        public Builder withGreetingsAck(byte[] greetings) {
            this.greetings = greetings;
            return this;
        }

        public RPCServerMock build() {
            return new RPCServerMock(port, greetings);
        }
    }
}
