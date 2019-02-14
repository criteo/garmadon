package com.criteo.hadoop.garmadon.agent;

import com.criteo.hadoop.garmadon.agent.utils.AsyncTestHelper;
import com.criteo.hadoop.garmadon.agent.utils.RPCServerMock;
import com.criteo.hadoop.garmadon.protocol.ProtocolVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.criteo.hadoop.garmadon.agent.utils.ObjectBuilderTestHelper.randomByteArray;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SocketAppenderTest {

    RPCServerMock server;

    @Before
    public void setUp() throws IOException {
        server = RPCServerMock.Builder
                .withPort(8080)
                .withGreetingsAck(ProtocolVersion.GREETINGS)
                .build();
        server.start();
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        server.shutdown();
    }

    @Test
    public void SocketAppender_should_establish_greetings_with_server() throws IOException {
        SocketAppender appender = new SocketAppender(new FixedConnection("localhost", 8080));
        appender.append("test".getBytes());

        assertThat(appender.isConnected(), is(true));
    }

    @Test
    public void SocketAppender_should_retry_establishing_connection() throws IOException, InterruptedException {
        server.shutdown();

        //We must restart server asynchronously because main thread will be stuck in reconnection
        Executors.newScheduledThreadPool(1).schedule(() -> server.start(), 1000, TimeUnit.MILLISECONDS);

        SocketAppender appender = new SocketAppender(new FixedConnection("localhost", 8080));
        appender.append("test".getBytes());

        assertThat(appender.isConnected(), is(true));
    }

    @Test
    public void SocketAppender_should_retry_establishing_connection_greet_response_not_expected() throws IOException, InterruptedException {
        //stop good server
        server.shutdown();

        //start bad server
        RPCServerMock badServer = RPCServerMock.Builder
                .withPort(8080)
                .withGreetingsAck("No greeting")
                .build();
        badServer.start();

        //stop bad server and restart good server
        Executors.newScheduledThreadPool(1).schedule(() -> {
            badServer.shutdown();
            server.start();
        } , 1000, TimeUnit.MILLISECONDS);

        SocketAppender appender = new SocketAppender(new FixedConnection("localhost", 8080));
        appender.append("test".getBytes());

        assertThat(appender.isConnected(), is(true));
    }

    @Test
    public void SocketAppender_should_retry_establishing_connection_if_no_response() throws IOException, InterruptedException {

    }

    @Test
    public void SocketAppender_should_retry_appending_until_get_connection() throws IOException, InterruptedException {
        server.shutdown();

        //We must restart server asynchronously because main thread will be stuck in reconnection
        Executors.newScheduledThreadPool(1).schedule(() -> server.start(), 1000, TimeUnit.MILLISECONDS);

        SocketAppender appender = new SocketAppender(new FixedConnection("localhost", 8080));
        appender.append(randomByteArray(200));

        AsyncTestHelper.tryDuring(2000, () -> server.verifyReceiveMsg());
    }

}