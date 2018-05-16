package com.criteo.hadoop.garmadon.forwarder.handler;

import com.criteo.hadoop.garmadon.event.proto.DataAccessEventProtos;
import com.criteo.hadoop.garmadon.forwarder.message.KafkaMessage;
import com.criteo.hadoop.garmadon.protocol.ProtocolMessage;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.events.StateEvent;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

public class CloseHandler extends ChannelInboundHandlerAdapter {

    private static AttributeKey<DataAccessEventProtos.Header> headerAttr = AttributeKey.valueOf("header");

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        DataAccessEventProtos.Header header = ctx.channel().attr(headerAttr).get();
        if (header != null && header.getTag().equals(Header.Tag.YARN_APPLICATION.name())) {
            StateEvent stateEvent = new StateEvent(System.currentTimeMillis(), StateEvent.State.END.toString());
            KafkaMessage kafkaMessage = new KafkaMessage(
                    header.getApplicationId(),
                    ProtocolMessage.create(header.toByteArray(), stateEvent)
            );

            ctx.fireChannelRead(kafkaMessage);
        }

        super.channelInactive(ctx);
    }
}
