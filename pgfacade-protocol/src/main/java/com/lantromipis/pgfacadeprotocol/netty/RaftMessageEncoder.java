package com.lantromipis.pgfacadeprotocol.netty;

import com.lantromipis.pgfacadeprotocol.message.AbstractMessage;
import com.lantromipis.pgfacadeprotocol.utils.MessageEncoderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

public class RaftMessageEncoder extends MessageToByteEncoder<AbstractMessage> {
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, AbstractMessage abstractMessage, ByteBuf byteBuf) throws Exception {
        MessageEncoderUtils.encodeMessage(abstractMessage, byteBuf);
    }
}
