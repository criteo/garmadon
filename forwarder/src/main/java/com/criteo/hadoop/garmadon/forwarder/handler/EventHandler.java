package com.criteo.hadoop.garmadon.forwarder.handler;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.forwarder.message.KafkaMessage;
import com.criteo.hadoop.garmadon.forwarder.metrics.PrometheusHttpMetrics;
import com.criteo.hadoop.garmadon.protocol.ProtocolConstants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private static final AttributeKey<EventHeaderProtos.Header> HEADER_ATTR = AttributeKey.valueOf("header");
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHandler.class);

    private boolean isFirst = true;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {

        int headerSize = msg.getInt(ProtocolConstants.HEADER_SIZE_INDEX);

        byte[] headerByte = new byte[headerSize];
        msg.getBytes(ProtocolConstants.FRAME_DELIMITER_SIZE, headerByte);

        EventHeaderProtos.Header header = EventHeaderProtos.Header.parseFrom(headerByte);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received event {} size {}", msg.getInt(0), msg.readableBytes());
        }

        byte[] raw = new byte[msg.readableBytes()];
        msg.readBytes(raw);

        KafkaMessage kafkaMessage = new KafkaMessage(
                header.getId(),
                raw);

        // Push header in context to sendAsync event end container
        if (isFirst) {
            ctx.channel().attr(HEADER_ATTR).set(header);
            isFirst = false;
        }

        ctx.fireChannelRead(kafkaMessage);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        PrometheusHttpMetrics.EVENTS_IN_ERROR.inc();
        LOGGER.error("", cause);
        ctx.close();
    }
}
