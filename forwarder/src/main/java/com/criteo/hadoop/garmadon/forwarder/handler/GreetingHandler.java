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

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        PrometheusHttpMetrics.GREETINGS_RECEIVED.inc();

        byte[] greetings = new byte[msg.readableBytes()];
        msg.readBytes(greetings);

        ProtocolVersion.checkVersion(greetings);

        ByteBuf pGreeting = Unpooled.buffer();
        pGreeting.writeBytes(ProtocolVersion.GREETINGS);
        ctx.writeAndFlush(pGreeting);

        ctx.pipeline().addBefore(KafkaHandler.class.getSimpleName(), CloseHandler.class.getSimpleName(), new CloseHandler());
        ctx.pipeline().remove(this);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        PrometheusHttpMetrics.GREETINGS_IN_ERROR.inc();
        LOGGER.error("", cause);
        ctx.close();
    }
}
