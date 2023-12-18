package com.lantromipis.postgresprotocol.handler.frontend;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.PgMessageInfo;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A special channel handler which can execute commands to Postgres using simple query protocol. Not thread safe.
 */
@Slf4j
public class PgChannelSimpleQueryExecutorHandler extends AbstractPgFrontendChannelHandler {

    private Deque<PgMessageInfo> messageInfos;
    private ByteBuf leftovers = null;

    private Consumer<Deque<PgMessageInfo>> responseCallback = null;
    private AtomicBoolean responseFulfilled = null;
    private AtomicBoolean resourcesFreed = new AtomicBoolean(false);


    public PgChannelSimpleQueryExecutorHandler() {
        this.messageInfos = new ArrayDeque<>();
    }

    public void executeQuery(String query, int timeoutMs, Consumer<Deque<PgMessageInfo>> callback) {
        responseFulfilled = new AtomicBoolean(false);
        resourcesFreed = new AtomicBoolean(false);
        responseCallback = callback;

        ByteBuf buf = ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage(query, initialChannelHandlerContext.alloc());
        initialChannelHandlerContext.channel().writeAndFlush(buf, initialChannelHandlerContext.voidPromise());
        initialChannelHandlerContext.channel().read();

        if (timeoutMs > 0) {
            initialChannelHandlerContext.executor().schedule(
                    () -> invokeCallback(null),
                    timeoutMs,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private void invokeCallback(Deque<PgMessageInfo> v) {
        if (responseCallback != null && responseFulfilled != null && responseFulfilled.compareAndSet(false, true)) {
            responseCallback.accept(v);
        }

        if (leftovers != null && resourcesFreed.compareAndSet(false, true)) {
            leftovers.release();
            DecoderUtils.freeMessageInfos(messageInfos);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;
        ByteBuf newLeftovers = DecoderUtils.splitToMessages(leftovers, message, messageInfos, ctx.alloc());

        if (leftovers != null) {
            leftovers.release();
        }
        leftovers = newLeftovers;

        if (responseCallback != null
                && responseFulfilled != null
                && DecoderUtils.containsMessageOfTypeReversed(messageInfos, PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR)) {
            invokeCallback(messageInfos);
        }

        ctx.channel().read();
        super.channelRead(ctx, msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        invokeCallback(null);

        HandlerUtils.closeOnFlush(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in PgChannelSimpleQueryExecutorHandler ", cause);

        invokeCallback(null);
        HandlerUtils.closeOnFlush(ctx.channel(), ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(ctx.alloc()));
    }
}
