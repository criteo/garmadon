package com.criteo.hadoop.garmadon.forwarder.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

//TODO the following class has to be removed when no more agent suffering
//https://github.com/criteo/garmadon/issues/17 run on the cluster
public class Discarder extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //release the bytebuffer just acts as reading and dropping the data
        ((ByteBuf) msg).release();
    }

}
