package com.lantromipis.connectionpool.handler;

import com.lantromipis.connectionpool.model.PgChannelCleanResult;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.handler.frontend.AbstractPgFrontendChannelHandler;
import com.lantromipis.postgresprotocol.model.internal.PgMessageInfo;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

/**
 * Cleans connection after client
 */
@Slf4j
public class PgChannelCleaningHandler extends AbstractPgFrontendChannelHandler {

    private final static ByteBuf ROLLBACK_SIMPLE_QUERY_MESSAGE_BYTE_BUF = Unpooled.unreleasableBuffer(
            ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage(
                    "rollback;",
                    UnpooledByteBufAllocator.DEFAULT
            )
    );

    private Consumer<PgChannelCleanResult> callback;
    private ByteBuf leftovers = null;
    private Deque<PgMessageInfo> pgMessageInfos;

    private int commandsCounter = 0;

    public PgChannelCleaningHandler(Consumer<PgChannelCleanResult> callback) {
        this.callback = callback;
        pgMessageInfos = new ArrayDeque<>();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().writeAndFlush(ROLLBACK_SIMPLE_QUERY_MESSAGE_BYTE_BUF);
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        @Cleanup("release") ByteBuf message = (ByteBuf) msg;

        ByteBuf newLeftovers = DecoderUtils.splitToMessages(leftovers, message, pgMessageInfos, ctx.alloc());

        if (leftovers != null) {
            leftovers.release();
        }
        leftovers = newLeftovers;

        if (DecoderUtils.containsMessageOfTypeReversed(pgMessageInfos, PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR)) {
            freeMessages();
            commandsCounter = -1;
            log.error("Failed to clean connection after client, because Postgres responded with error");
            callback.accept(new PgChannelCleanResult(false));
            return;
        }

        // 0 == rollback, 1 == deallocate all.
        switch (commandsCounter) {
            case 0 -> {
                if (DecoderUtils.containsMessageOfTypeReversed(pgMessageInfos, PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR)) {
                    freeMessages();
                    commandsCounter++;
                    ctx.channel().writeAndFlush(ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage("deallocate all;", ctx.alloc()));
                }
            }
            case 1 -> {
                if (DecoderUtils.containsMessageOfTypeReversed(pgMessageInfos, PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR)) {
                    freeMessages();
                    callback.accept(new PgChannelCleanResult(true));
                    commandsCounter++;
                }
            }
        }

        ctx.channel().read();
        super.channelRead(ctx, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.channel().close();
        callback.accept(new PgChannelCleanResult(false));
        log.error("Failed to clean connection after client", cause);
    }

    private void freeMessages() {
        DecoderUtils.freeMessageInfos(pgMessageInfos);

        if (leftovers != null) {
            leftovers.release();
            leftovers = null;
        }
    }
}
