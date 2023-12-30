package com.lantromipis.postgresprotocol.handler.frontend;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.decoder.ServerPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.PgChannelAuthResult;
import com.lantromipis.postgresprotocol.model.internal.PgMessageInfo;
import com.lantromipis.postgresprotocol.model.protocol.ErrorResponse;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Slf4j
public class PgChannelAfterAuthHandler extends AbstractPgFrontendChannelHandler {

    private final Consumer<PgChannelAuthResult> callbackFunction;
    private final Deque<PgMessageInfo> pgMessageInfos;
    private ByteBuf leftovers = null;
    private AtomicBoolean resourcesFreed = new AtomicBoolean(false);

    public PgChannelAfterAuthHandler(final Consumer<PgChannelAuthResult> callbackFunction) {
        this.callbackFunction = callbackFunction;
        this.pgMessageInfos = new ArrayDeque<>();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        if (resourcesFreed.compareAndSet(false, true)) {
            if (leftovers != null) {
                leftovers.release();
            }
            DecoderUtils.freeMessageInfos(pgMessageInfos);
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
        ctx.channel().read();
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
            AtomicReference<ErrorResponse> errorResponse = new AtomicReference<>(null);
            DecoderUtils.processSplitMessages(pgMessageInfos, pgMessageInfo -> {
                        if (pgMessageInfo.getStartByte() == PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR) {
                            errorResponse.set(ServerPostgresProtocolMessageDecoder.decodeErrorResponse(pgMessageInfo.getEntireMessage()));
                            return true;
                        }
                        return false;
                    }
            );
            callbackFunction.accept(new PgChannelAuthResult(errorResponse.get()));
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

            ctx.channel().pipeline().remove(this);
            callbackFunction.accept(new PgChannelAuthResult(containsAuthOk, pgMessageInfos));
            return;
        }

        ctx.channel().read();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception during final stage of Postgres channel auth!", cause);
        callbackFunction.accept(new PgChannelAuthResult(false));
        HandlerUtils.closeOnFlush(ctx.channel(), ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(ctx.alloc()));
    }
}
