package com.lantromipis.connectionpool.handler.common;

import com.lantromipis.connectionpool.model.PgChannelAuthResult;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class PgChannelAfterAuthHandler extends AbstractConnectionPoolClientHandler {

    private final Consumer<PgChannelAuthResult> callbackFunction;
    private final List<MessageInfo> messageInfos;
    private ByteBuf leftovers = null;

    public PgChannelAfterAuthHandler(final Consumer<PgChannelAuthResult> callbackFunction) {
        this.callbackFunction = callbackFunction;
        this.messageInfos = new ArrayList<>();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().read();
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        leftovers = DecoderUtils.splitToMessages(leftovers, message, messageInfos);

        if (DecoderUtils.containsMessageOfTypeReversed(messageInfos, PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR)) {
            callbackFunction.accept(
                    new PgChannelAuthResult(false)
            );
            ctx.channel().pipeline().remove(this);
            return;
        }

        if (leftovers == null || leftovers.readableBytes() == 0) {
            boolean containsAuthOk = false;

            for (int i = messageInfos.size() - 1; i >= 0; i--) {
                MessageInfo messageInfo = messageInfos.get(i);

                if (messageInfo.getStartByte() == PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR
                        && messageInfo.getLength() == PostgresProtocolGeneralConstants.AUTH_OK_MESSAGE_LENGTH) {

                    ByteBuf byteBuf = ctx.alloc().buffer(messageInfo.getEntireMessage().length);
                    byteBuf.writeBytes(messageInfo.getEntireMessage());

                    // remove start byte and length
                    byteBuf.readByte();
                    byteBuf.readInt();

                    int data = byteBuf.readInt();

                    byteBuf.release();

                    if (data == PostgresProtocolGeneralConstants.AUTH_OK_MESSAGE_DATA) {
                        containsAuthOk = true;
                        break;
                    }
                }
            }


            callbackFunction.accept(
                    new PgChannelAuthResult(containsAuthOk, messageInfos)
            );
            ctx.channel().pipeline().remove(this);
            return;
        }

        ctx.channel().read();
    }
}
