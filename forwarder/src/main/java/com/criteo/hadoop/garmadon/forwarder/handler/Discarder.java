package com.criteo.hadoop.garmadon.forwarder.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class Discarder extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //release the bytebuffer just acts as reading and dropping the data
        ((ByteBuf) msg).release();
    }

}
