package com.criteo.hadoop.garmadon.forwarder.handler;

import com.criteo.hadoop.garmadon.forwarder.metrics.PrometheusHttpMetrics;
import com.criteo.hadoop.garmadon.protocol.ProtocolVersion;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GreetingHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GreetingHandler.class);

    private byte[] lastAgentGreetings;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        PrometheusHttpMetrics.GREETINGS_RECEIVED.inc();

        byte[] greetings = new byte[msg.readableBytes()];
        msg.readBytes(greetings);

        lastAgentGreetings = greetings;

        ProtocolVersion.checkVersion(greetings);

        sendValidGreetings(ctx, ProtocolVersion.GREETINGS);

        ctx.pipeline().addBefore(KafkaHandler.class.getSimpleName(), CloseHandler.class.getSimpleName(), new CloseHandler());
        ctx.pipeline().remove(this);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        PrometheusHttpMetrics.GREETINGS_IN_ERROR.inc();
        LOGGER.error("", cause);

        //TODO the following code has to be removed when no more agent suffering
        //https://github.com/criteo/garmadon/issues/17 run on the cluster
        if (cause instanceof ProtocolVersion.InvalidProtocolVersionException) {
            //We make the agent think it can talk to us. For that we need to return its protocol version
            sendValidGreetings(ctx, lastAgentGreetings);

            //Don't close the connection but ignore sent data to the forwarder and handle close
            ctx.pipeline().addFirst(CloseHandler.class.getSimpleName(), new CloseHandler());
            ctx.pipeline().addFirst(Discarder.class.getSimpleName(), new Discarder());
        } else {
            //otherwise it could be another process so close the connection
            ctx.close();
        }
    }

    private void sendValidGreetings(ChannelHandlerContext ctx, byte[] greeting) {
        ByteBuf pGreeting = Unpooled.buffer();
        pGreeting.writeBytes(greeting);
        ctx.writeAndFlush(pGreeting);
    }
}
