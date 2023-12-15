package com.lantromipis.connectionpool.handler.common;

import com.lantromipis.connectionpool.model.PgChannelCleanResult;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

/**
 * Cleans connection after client
 */
@Slf4j
public class PgChannelCleaningHandler extends AbstractConnectionPoolClientHandler {

    private Consumer<PgChannelCleanResult> callback;
    private ByteBuf leftovers = null;
    private Deque<MessageInfo> messageInfos;

    private int commandsCounter = 0;

    public PgChannelCleaningHandler(Consumer<PgChannelCleanResult> callback) {
        this.callback = callback;
        messageInfos = new ArrayDeque<>();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().writeAndFlush(ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage("rollback;", ctx.alloc()));
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
            freeMessages();
            commandsCounter = -1;
            log.error("Failed to clean connection after client, because Postgres responded with error");
            callback.accept(new PgChannelCleanResult(false));
            return;
        }

        // 0 == rollback, 1 == deallocate all.
        switch (commandsCounter) {
            case 0 -> {
                if (DecoderUtils.containsMessageOfTypeReversed(messageInfos, PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR)) {
                    freeMessages();
                    commandsCounter++;
                    ctx.channel().writeAndFlush(ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage("deallocate all;", ctx.alloc()));
                }
            }
            case 1 -> {
                if (DecoderUtils.containsMessageOfTypeReversed(messageInfos, PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR)) {
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
        DecoderUtils.freeMessageInfos(messageInfos);

        if (leftovers != null) {
            leftovers.release();
            leftovers = null;
        }
    }
}
