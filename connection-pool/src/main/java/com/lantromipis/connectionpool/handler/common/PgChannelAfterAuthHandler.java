package com.lantromipis.connectionpool.handler.common;

import com.lantromipis.connectionpool.model.PgChannelAuthResult;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import com.lantromipis.postgresprotocol.model.internal.SplitResult;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class PgChannelAfterAuthHandler extends AbstractConnectionPoolClientHandler {

    private final Function<PgChannelAuthResult, Void> callbackFunction;
    private final List<MessageInfo> messageInfos;
    private ByteBuf leftovers = null;

    public PgChannelAfterAuthHandler(final Function<PgChannelAuthResult, Void> callbackFunction) {
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

        SplitResult splitResult = DecoderUtils.splitToMessages(leftovers, message);

        if (DecoderUtils.containsMessageOfType(splitResult.getMessageInfos(), PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR)) {
            callbackFunction.apply(
                    new PgChannelAuthResult(false)
            );
            ctx.channel().pipeline().remove(this);
            return;
        }

        messageInfos.addAll(splitResult.getMessageInfos());

        if (splitResult.getLastIncompleteMessage() == null || splitResult.getLastIncompleteMessage().readableBytes() == 0) {
            boolean containsAuthOk = messageInfos.stream()
                    .anyMatch(messageInfo -> {
                        if (messageInfo.getStartByte() == PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR
                                && messageInfo.getLength() == PostgresProtocolGeneralConstants.AUTH_OK_MESSAGE_LENGTH) {
                            // remove start byte and length
                            messageInfo.getEntireMessage().readByte();
                            messageInfo.getEntireMessage().readInt();

                            int data = messageInfo.getEntireMessage().readInt();

                            return data == PostgresProtocolGeneralConstants.AUTH_OK_MESSAGE_DATA;
                        }

                        return false;
                    });


            callbackFunction.apply(
                    new PgChannelAuthResult(containsAuthOk, messageInfos)
            );
            ctx.channel().pipeline().remove(this);
            return;
        }

        leftovers = splitResult.getLastIncompleteMessage();

        ctx.channel().read();
    }
}
