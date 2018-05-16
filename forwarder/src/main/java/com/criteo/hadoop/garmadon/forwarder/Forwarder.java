package com.criteo.hadoop.garmadon.forwarder;

import com.criteo.hadoop.garmadon.forwarder.channel.ForwarderChannelInitializer;
import com.criteo.hadoop.garmadon.forwarder.kafka.KafkaService;
import com.criteo.hadoop.garmadon.forwarder.metrics.ForwarderEventSender;
import com.criteo.hadoop.garmadon.forwarder.metrics.HostStatistics;
import com.criteo.hadoop.garmadon.forwarder.metrics.PrometheusHttpMetrics;
import com.criteo.hadoop.garmadon.schema.events.Header;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class Forwarder {
    private static final Logger logger = LoggerFactory.getLogger(Forwarder.class);

    private static final String DEFAULT_FORWARDER_PORT = "33000";
    private static final String DEFAULT_PROMETHEUS_PORT = "33001";

    public static String hostname;
    static {
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error("",e);
        }
    }

    private final Properties properties;


    private final byte[] header;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private Channel serverChannel;
    private KafkaService kafkaService;

    public Forwarder(Properties properties) {
        this.properties = properties;
        this.header = Header.newBuilder()
                .withHostname(hostname)
                .withTag(Header.Tag.FORWARDER.name())
                .build()
                .serialize();
    }

    /**
     * Starts netty server for forwarder
     *
     * @return a ChannelFuture that completes when server is started.
     * @throws UnknownHostException
     */
    public ChannelFuture run() throws IOException {
        // initialise kafka
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "garmadon.forwarder." + hostname);
        kafkaService = new KafkaService(properties);

        // initialize metrics
        int prometheusPort = Integer.parseInt(properties.getProperty("prometheus.port", DEFAULT_PROMETHEUS_PORT));
        PrometheusHttpMetrics.start(prometheusPort);
        ForwarderEventSender forwarderEventSender = new ForwarderEventSender(kafkaService, hostname, header);
        HostStatistics.startReport(forwarderEventSender);

        //initialize netty
        int forwarderPort = Integer.parseInt(properties.getProperty("forwarder.port", DEFAULT_FORWARDER_PORT));
        return startNetty(forwarderPort);
    }

    /**
     * Closes netty server (in a blocking fashion)
     *
     * @return a ChannelFuture that completes when server is closed
     */
    public void close() {
        logger.info("Shutdown netty server");
        if (serverChannel == null) {
            logger.error("Cannot close a non running server");
            throw new IllegalStateException("Cannot close a non running server");
        }

        serverChannel.close().syncUninterruptibly();
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().syncUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().syncUninterruptibly();
        }

        HostStatistics.stopReport();

        kafkaService.shutdown();

        PrometheusHttpMetrics.stop();
    }

    private ChannelFuture startNetty(int port) {
        int workerThreads = Integer.parseInt(properties.getProperty("forwarder.worker.thread", "1"));

        // Setup netty listener
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(workerThreads);

        //setup boostrap
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                // TODO: Test the Unix Domain Socket implementation will need junixsocket at client side....
                // But should increase perf
                //.channel(EpollServerDomainSocketChannel.class)
                .childHandler(new ForwarderChannelInitializer(kafkaService));

        //start server
        logger.info("Startup netty server");
        ChannelFuture f = b.bind("localhost", port).addListener(future -> logger.info("Netty server started"));
        serverChannel = f.channel();
        return f;
    }

    public static void main(String[] args) throws Exception {

        // Get properties
        Properties properties = new Properties();
        try (InputStream streamPropFilePath = Forwarder.class.getResourceAsStream("/server.properties")) {
            properties.load(streamPropFilePath);
        }
        //start server and wait for completion (for now we must kill process)
        Forwarder forwarder = new Forwarder(properties);

        // Add ShutdownHook
        Runtime.getRuntime().addShutdownHook(new Thread(forwarder::close));

        try {
            forwarder.run().channel().closeFuture().sync();
        } finally {
            forwarder.close();
        }
    }
}
