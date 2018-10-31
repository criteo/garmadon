package com.criteo.hadoop.garmadon.forwarder.handler;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.forwarder.message.KafkaMessage;
import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.enums.State;
import com.criteo.hadoop.garmadon.schema.events.Header;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

public class CloseHandler extends ChannelInboundHandlerAdapter {

    private static AttributeKey<EventHeaderProtos.Header> headerAttr = AttributeKey.valueOf("header");

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        EventHeaderProtos.Header header = ctx.channel().attr(headerAttr).get();
        if (header != null && header.getTagsList().contains(Header.Tag.YARN_APPLICATION.name())) {
            DataAccessEventProtos.StateEvent stateEvent = DataAccessEventProtos.StateEvent
                    .newBuilder()
                    .setState(State.END.name())
                    .build();

            KafkaMessage kafkaMessage = new KafkaMessage(
                    header.getApplicationId(),
                    ProtocolMessage.create(System.currentTimeMillis(), header.toByteArray(), stateEvent)
            );

            ctx.fireChannelRead(kafkaMessage);
        }

        super.channelInactive(ctx);
    }
}
