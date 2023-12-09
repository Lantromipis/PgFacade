package com.lantromipis.connectionpool.handler.common;

import com.lantromipis.connectionpool.model.PgChannelCleanResult;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Cleans connection after client
 */
@Slf4j
public class PgChannelCleaningHandler extends AbstractConnectionPoolClientHandler {

    private Consumer<PgChannelCleanResult> callback;
    private ByteBuf leftovers;
    private List<MessageInfo> messageInfos;

    private boolean ready;

    public PgChannelCleaningHandler(Consumer<PgChannelCleanResult> callback) {
        this.callback = callback;
        messageInfos = new ArrayList<>();
        ready = false;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().writeAndFlush(ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage("rollback;"));
        ctx.channel().writeAndFlush(ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage("deallocate all;"));
        ready = true;
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;
        leftovers = DecoderUtils.splitToMessages(leftovers, message, messageInfos);

        if (ready && DecoderUtils.containsMessageOfTypeReversed(messageInfos, PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR)) {
            callback.accept(new PgChannelCleanResult(true));
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
}
