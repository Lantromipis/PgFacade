package com.lantromipis.postgresprotocol.handler.frontend;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.decoder.ServerPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.PgMessageInfo;
import com.lantromipis.postgresprotocol.model.protocol.ErrorResponse;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.postgresprotocol.utils.PostgresHandlerUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * A special channel handler which can execute commands to Postgres using simple query protocol.
 * Not thread safe, so new command can be executed only when previous completed.
 * If no command is issued, this handler passes all messages to other handlers in pipeline.
 */
@Slf4j
public class PgChannelSimpleQueryExecutorHandler extends AbstractPgFrontendChannelHandler {

    private Deque<PgMessageInfo> messageInfos = new ArrayDeque<>();
    private ByteBuf leftovers = null;

    private Consumer<CommandExecutionResult> responseCallback = null;
    private AtomicBoolean responseFulfilled = null;
    private ScheduledFuture<?> cancelFuture = null;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CommandExecutionResult {
        private CommandExecutionResultStatus status;
        private Deque<PgMessageInfo> messageInfos;
        private ErrorResponse errorResponse;
        private Throwable throwable;
    }

    public enum CommandExecutionResultStatus {
        SUCCESS,
        SERVER_ERROR,
        CLIENT_ERROR,
        TIMEOUT
    }

    public PgChannelSimpleQueryExecutorHandler() {
        super();
    }

    public PgChannelSimpleQueryExecutorHandler(CountDownLatch readyCountDownLatch) {
        super(readyCountDownLatch);
    }

    /**
     * Executes provided SQL query using simple query protocol AND waits for response from Postgresql.
     * <p>
     * <b> Note1: caller MUST release all messages in provided result </b>
     * <p>
     * <b> Note2: if timeout received, it is not safe to use this handler and channel anymore. Even if Postgres server is up, TCP packets after timeout for this request can broke entire channel pipeline! </b>
     *
     * @param query     SQL query to execute
     * @param timeoutMs timeout for query to execute
     * @return query execution result
     */
    public CommandExecutionResult executeQueryBlocking(String query, long timeoutMs) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<CommandExecutionResult> ret = new AtomicReference<>();

        executeQuery(
                query,
                timeoutMs,
                result -> {
                    ret.set(result);
                    countDownLatch.countDown();
                }
        );

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Thread interrupted while waiting for query " + query + " to complete", e);
            return CommandExecutionResult
                    .builder()
                    .status(CommandExecutionResultStatus.CLIENT_ERROR)
                    .build();
        }
        return ret.get();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        if (leftovers != null && leftovers.refCnt() > 0) {
            leftovers.release();
        }
        DecoderUtils.freeMessageInfos(messageInfos);
    }

    /**
     * Executes provided SQL query using simple query protocol. Once request is fulfilled, provided callback is called
     * <p>
     * <b> Note1: caller MUST release all messages in provided result </b>
     * <p>
     * <b> Note2: if timeout received, it is not safe to use this handler and channel anymore. Even if Postgres server is up, TCP packets after timeout for this request can broke entire channel pipeline! </b>
     *
     * @param query     SQL query to execute
     * @param timeoutMs timeout for query to execute
     * @param callback  callback to call once response from Postgres received or timeout reached
     */
    public void executeQuery(String query, long timeoutMs, Consumer<CommandExecutionResult> callback) {
        responseFulfilled = new AtomicBoolean(false);
        responseCallback = callback;
        cancelFuture = null;

        DecoderUtils.freeMessageInfos(messageInfos);
        if (leftovers != null && leftovers.refCnt() > 0) {
            leftovers.release();
        }

        ByteBuf buf = ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage(query, initialChannelHandlerContext.alloc());
        initialChannelHandlerContext.channel().writeAndFlush(buf, initialChannelHandlerContext.voidPromise());
        initialChannelHandlerContext.channel().read();

        if (timeoutMs > 0) {
            cancelFuture = initialChannelHandlerContext.executor().schedule(
                    () -> {
                        log.debug("Canceling query '{}' execution due to timeout!", query);
                        invokeCallback(
                                CommandExecutionResult
                                        .builder()
                                        .status(CommandExecutionResultStatus.TIMEOUT)
                                        .build()
                        );
                    },
                    timeoutMs,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private boolean invokeCallback(CommandExecutionResult commandExecutionResult) {
        if (responseCallback != null && responseFulfilled != null && responseFulfilled.compareAndSet(false, true)) {
            responseCallback.accept(commandExecutionResult);
            responseCallback = null;
            if (cancelFuture != null) {
                cancelFuture.cancel(false);
            }
            return true;
        }

        return false;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // pass message to another handler
        if (responseCallback == null || responseFulfilled == null || responseFulfilled.get()) {
            ctx.fireChannelRead(msg);
            return;
        }

        @Cleanup("release") ByteBuf message = (ByteBuf) msg;
        ByteBuf newLeftovers = DecoderUtils.splitToMessages(leftovers, message, messageInfos, ctx.alloc());

        if (leftovers != null) {
            leftovers.release();
        }
        leftovers = newLeftovers;

        // handle error
        if (leftovers == null && DecoderUtils.containsMessageOfTypeReversed(messageInfos, PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR)) {
            DecoderUtils.processSplitMessages(
                    messageInfos,
                    messageInfo -> {
                        if (messageInfo.getStartByte() == PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR) {
                            ErrorResponse errorResponse = ServerPostgresProtocolMessageDecoder.decodeErrorResponse(messageInfo.getEntireMessage());
                            invokeCallback(
                                    CommandExecutionResult
                                            .builder()
                                            .status(CommandExecutionResultStatus.SERVER_ERROR)
                                            .errorResponse(errorResponse)
                                            .build()
                            );
                            return true;
                        }
                        return false;
                    }
            );
            // release ByteBufs and clear Deque
            DecoderUtils.freeMessageInfos(messageInfos);
        }

        // handle success
        if (leftovers == null && DecoderUtils.containsMessageOfTypeReversed(messageInfos, PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR)) {
            invokeCallback(
                    CommandExecutionResult
                            .builder()
                            .status(CommandExecutionResultStatus.SUCCESS)
                            .messageInfos(new ArrayDeque<>(messageInfos))
                            .build()
            );
            messageInfos.clear();
        }

        ctx.channel().read();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        invokeCallback(
                CommandExecutionResult
                        .builder()
                        .status(CommandExecutionResultStatus.TIMEOUT)
                        .build()
        );

        PostgresHandlerUtils.closeOnFlush(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        boolean invoked = invokeCallback(
                CommandExecutionResult
                        .builder()
                        .status(CommandExecutionResultStatus.CLIENT_ERROR)
                        .throwable(cause)
                        .build()
        );

        if (!invoked) {
            if (ctx.pipeline().toMap().size() == 1) {
                log.error("Exception in PgChannelSimpleQueryExecutorHandler ", cause);
                PostgresHandlerUtils.closeOnFlush(ctx.channel(), ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(ctx.alloc()));
            } else {
                super.exceptionCaught(ctx, cause);
            }
        } else {
            PostgresHandlerUtils.closeOnFlush(ctx.channel(), ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(ctx.alloc()));
        }
    }
}
