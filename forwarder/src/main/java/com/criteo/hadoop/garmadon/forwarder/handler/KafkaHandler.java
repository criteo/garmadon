package com.criteo.hadoop.garmadon.forwarder.handler;

import com.criteo.hadoop.garmadon.forwarder.kafka.KafkaService;
import com.criteo.hadoop.garmadon.forwarder.message.KafkaMessage;
import com.criteo.hadoop.garmadon.forwarder.metrics.PrometheusHttpMetrics;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class KafkaHandler extends SimpleChannelInboundHandler<KafkaMessage> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaHandler.class);

    private KafkaService kafkaService;

    public KafkaHandler(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KafkaMessage msg) {
        PrometheusHttpMetrics.EVENTS_RECEIVED.inc();

        int type = ByteBuffer.wrap(msg.getValue(), 0, 4).getInt(0);
        PrometheusHttpMetrics.eventSize(type).observe(msg.getValue().length);

        kafkaService.sendRecordAsync(msg.getKey(), msg.getValue());
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.error("", cause);
        ctx.close();
    }
}
