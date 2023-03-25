package com.lantromipis.pgfacadeprotocol.netty;

import com.lantromipis.pgfacadeprotocol.utils.MessageDecoderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class RaftMessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        list.add(MessageDecoderUtils.decodeMessage(byteBuf));
    }
}
