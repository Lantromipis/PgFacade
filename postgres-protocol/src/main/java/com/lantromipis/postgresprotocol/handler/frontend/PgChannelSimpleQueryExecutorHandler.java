package com.lantromipis.postgresprotocol.handler.frontend;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.decoder.ServerPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.PgMessageInfo;
import com.lantromipis.postgresprotocol.model.protocol.ErrorResponse;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
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

    public CommandExecutionResult executeQueryBlocking(String query, int timeoutMs) {
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
            log.info("Thread interrupted", e);
            return CommandExecutionResult
                    .builder()
                    .status(CommandExecutionResultStatus.CLIENT_ERROR)
                    .build();
        }
        return ret.get();
    }

    public void executeQuery(String query, int timeoutMs, Consumer<CommandExecutionResult> callback) {
        responseFulfilled = new AtomicBoolean(false);
        responseCallback = callback;

        DecoderUtils.freeMessageInfos(messageInfos);
        if (leftovers != null && leftovers.refCnt() > 0) {
            leftovers.release();
        }

        ByteBuf buf = ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage(query, initialChannelHandlerContext.alloc());
        initialChannelHandlerContext.channel().writeAndFlush(buf, initialChannelHandlerContext.voidPromise());
        initialChannelHandlerContext.channel().read();

        if (timeoutMs > 0) {
            initialChannelHandlerContext.executor().schedule(
                    () -> invokeCallback(
                            CommandExecutionResult
                                    .builder()
                                    .status(CommandExecutionResultStatus.TIMEOUT)
                                    .build()
                    ),
                    timeoutMs,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private boolean invokeCallback(CommandExecutionResult commandExecutionResult) {
        if (responseCallback != null && responseFulfilled != null && responseFulfilled.compareAndSet(false, true)) {
            responseCallback.accept(commandExecutionResult);
            responseCallback = null;
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

        ByteBuf message = (ByteBuf) msg;
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
        }

        // handle success
        if (leftovers == null && DecoderUtils.containsMessageOfTypeReversed(messageInfos, PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR)) {
            invokeCallback(
                    CommandExecutionResult
                            .builder()
                            .status(CommandExecutionResultStatus.SUCCESS)
                            .messageInfos(messageInfos)
                            .build()
            );
        }

        ctx.channel().read();
        super.channelRead(ctx, msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        invokeCallback(
                CommandExecutionResult
                        .builder()
                        .status(CommandExecutionResultStatus.TIMEOUT)
                        .build()
        );

        HandlerUtils.closeOnFlush(ctx.channel());
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
            log.error("Exception in PgChannelSimpleQueryExecutorHandler ", cause);
        }
        HandlerUtils.closeOnFlush(ctx.channel(), ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(ctx.alloc()));
    }
}
