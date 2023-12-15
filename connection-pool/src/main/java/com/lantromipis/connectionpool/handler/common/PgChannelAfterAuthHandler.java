package com.lantromipis.connectionpool.handler.common;

import com.lantromipis.connectionpool.model.PgChannelAuthResult;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

public class PgChannelAfterAuthHandler extends AbstractConnectionPoolClientHandler {

    private final Consumer<PgChannelAuthResult> callbackFunction;
    private final Deque<MessageInfo> messageInfos;
    private ByteBuf leftovers = null;

    public PgChannelAfterAuthHandler(final Consumer<PgChannelAuthResult> callbackFunction) {
        this.callbackFunction = callbackFunction;
        this.messageInfos = new ArrayDeque<>();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().read();
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        ByteBuf newLeftovers = DecoderUtils.splitToMessages(leftovers, message, messageInfos, ctx.alloc());

        if (leftovers != null) {
            leftovers.release();
        }
        leftovers = newLeftovers;

        if (DecoderUtils.containsMessageOfTypeReversed(messageInfos, PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR)) {
            callbackFunction.accept(new PgChannelAuthResult(false));
            ctx.channel().pipeline().remove(this);
            return;
        }

        if (leftovers == null || leftovers.readableBytes() == 0) {
            boolean containsAuthOk = false;

            MessageInfo messageInfo = messageInfos.poll();
            while (messageInfo != null) {
                if (messageInfo.getStartByte() == PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR
                        && messageInfo.getLength() == PostgresProtocolGeneralConstants.AUTH_OK_MESSAGE_LENGTH) {
                    ByteBuf messageBytes = messageInfo.getEntireMessage();

                    // 1 byte start byte + 4 bytes length
                    messageBytes.readerIndex(5);

                    if (messageBytes.readInt() == PostgresProtocolGeneralConstants.AUTH_OK_MESSAGE_DATA) {
                        containsAuthOk = true;
                        break;
                    }
                }

                messageInfo.getEntireMessage().release();
                messageInfo = messageInfos.poll();
            }

            callbackFunction.accept(new PgChannelAuthResult(containsAuthOk, messageInfos));

            if (leftovers != null) {
                leftovers.release();
            }
            DecoderUtils.freeMessageInfos(messageInfos);

            ctx.channel().pipeline().remove(this);
            return;
        }

        ctx.channel().read();
    }
}
