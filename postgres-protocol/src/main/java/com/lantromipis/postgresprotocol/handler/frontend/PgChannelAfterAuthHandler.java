package com.lantromipis.postgresprotocol.handler.frontend;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.internal.PgChannelAuthResult;
import com.lantromipis.postgresprotocol.model.internal.PgMessageInfo;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

public class PgChannelAfterAuthHandler extends AbstractPgFrontendChannelHandler {

    private final Consumer<PgChannelAuthResult> callbackFunction;
    private final Deque<PgMessageInfo> pgMessageInfos;
    private ByteBuf leftovers = null;

    public PgChannelAfterAuthHandler(final Consumer<PgChannelAuthResult> callbackFunction) {
        this.callbackFunction = callbackFunction;
        this.pgMessageInfos = new ArrayDeque<>();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().read();
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        ByteBuf newLeftovers = DecoderUtils.splitToMessages(leftovers, message, pgMessageInfos, ctx.alloc());

        if (leftovers != null) {
            leftovers.release();
        }
        leftovers = newLeftovers;

        if (DecoderUtils.containsMessageOfTypeReversed(pgMessageInfos, PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR)) {
            callbackFunction.accept(new PgChannelAuthResult(false));
            ctx.channel().pipeline().remove(this);
            return;
        }

        if (leftovers == null || leftovers.readableBytes() == 0) {
            boolean containsAuthOk = false;

            PgMessageInfo pgMessageInfo = pgMessageInfos.poll();
            while (pgMessageInfo != null) {
                if (pgMessageInfo.getStartByte() == PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR
                        && pgMessageInfo.getLength() == PostgresProtocolGeneralConstants.AUTH_OK_MESSAGE_LENGTH) {
                    ByteBuf messageBytes = pgMessageInfo.getEntireMessage();

                    // 1 byte start byte + 4 bytes length
                    messageBytes.readerIndex(5);

                    if (messageBytes.readInt() == PostgresProtocolGeneralConstants.AUTH_OK_MESSAGE_DATA) {
                        containsAuthOk = true;
                        break;
                    }
                }

                pgMessageInfo.getEntireMessage().release();
                pgMessageInfo = pgMessageInfos.poll();
            }

            callbackFunction.accept(new PgChannelAuthResult(containsAuthOk, pgMessageInfos));

            if (leftovers != null) {
                leftovers.release();
            }
            DecoderUtils.freeMessageInfos(pgMessageInfos);

            ctx.channel().pipeline().remove(this);
            return;
        }

        ctx.channel().read();
    }
}
