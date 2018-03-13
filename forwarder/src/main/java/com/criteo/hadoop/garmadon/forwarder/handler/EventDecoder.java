package com.criteo.hadoop.garmadon.forwarder.handler;

import com.criteo.hadoop.garmadon.protocol.ProtocolConstants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

public class EventDecoder extends ReplayingDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) {
        int offset = buf.readerIndex();
        int headerSize = buf.getInt(ProtocolConstants.HEADER_SIZE_INDEX + offset);
        int bodySize = buf.getInt(ProtocolConstants.BODY_SIZE_INDEX + offset);
        out.add(buf.readBytes(ProtocolConstants.FRAME_DELIMITER_SIZE + headerSize + bodySize));
    }
}
