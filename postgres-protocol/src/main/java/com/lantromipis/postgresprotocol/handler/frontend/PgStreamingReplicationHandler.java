package com.lantromipis.postgresprotocol.handler.frontend;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolStreamingReplicationConstants;
import com.lantromipis.postgresprotocol.decoder.ServerPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.PgLogSequenceNumber;
import com.lantromipis.postgresprotocol.model.protocol.ErrorResponse;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.postgresprotocol.utils.PostgresTimeUtils;
import com.lantromipis.postgresprotocol.utils.TempFastThreadLocalStorageUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
public class PgStreamingReplicationHandler extends AbstractPgFrontendChannelHandler {

    private static final byte FAKE_COPY_DATA_MESSAGE_START_BYTE = 0;

    private boolean replicationStarted = false;
    private Consumer<ReplicationStartResult> startedCallback;
    private Consumer<ReplicationErrorResult> errorCallback;
    private Consumer<WalFragmentReceivedResult> walFragmentReceivedCallback;
    private Runnable streamingCompleted;

    private boolean readingCopyDataMessage = false;
    private byte currentCopyDataMessageStartByte = FAKE_COPY_DATA_MESSAGE_START_BYTE;
    private int currentCopyDataMessageLength = 0;
    private int leftToReadFromCopyData = 0;
    private boolean finishedReadingCopyDataMessageStartInfo = false;

    private PgLogSequenceNumber lastFragmentStartLsn = PgLogSequenceNumber.INVALID_LSN;
    private PgLogSequenceNumber lastReceivedLsn = PgLogSequenceNumber.INVALID_LSN;
    private PgLogSequenceNumber lastFlushedLsn = PgLogSequenceNumber.INVALID_LSN;
    private PgLogSequenceNumber lastAppliedLsn = PgLogSequenceNumber.INVALID_LSN;

    private String timeline;
    private String slotName;
    private long updateIntervalMs;
    private ScheduledFuture<?> keepaliveFuture;

    private ByteBuf internalByteBuf;

    private AtomicBoolean resourcesFreed;
    private boolean copyDone;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReplicationStartResult {
        private boolean success;
        private boolean serverError;
        private ErrorResponse errorResponse;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReplicationErrorResult {
        private boolean serverError;
        private boolean exception;
        private ErrorResponse errorResponse;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WalFragmentReceivedResult {
        private PgLogSequenceNumber fragmentStartLsn;
        private byte[] fragment;
        private int fragmentLength;
    }


    public void startPhysicalReplication(final String slotName,
                                         final String startLsn,
                                         final String timeline,
                                         final long updateIntervalMs,
                                         final Consumer<ReplicationStartResult> startedCallback,
                                         final Consumer<ReplicationErrorResult> errorCallback,
                                         final Consumer<WalFragmentReceivedResult> walFragmentReceivedCallback,
                                         final Runnable streamingCompleted) {
        internalByteBuf = initialChannelHandlerContext.alloc().buffer(2048);
        resourcesFreed = new AtomicBoolean(false);

        this.startedCallback = startedCallback;
        this.errorCallback = errorCallback;
        this.walFragmentReceivedCallback = walFragmentReceivedCallback;
        this.timeline = timeline;
        this.slotName = slotName;
        this.updateIntervalMs = updateIntervalMs;
        this.streamingCompleted = streamingCompleted;
        this.copyDone = false;

        StringBuilder query = new StringBuilder("START_REPLICATION");

        if (StringUtils.isNotEmpty(slotName)) {
            query.append(" ").append("SLOT").append(" ").append(slotName);
        }

        query.append(" ").append("PHYSICAL");
        query.append(" ").append(startLsn);
        lastReceivedLsn = PgLogSequenceNumber.valueOf(startLsn);
        lastAppliedLsn = lastReceivedLsn;
        lastFlushedLsn = lastReceivedLsn;

        if (StringUtils.isNotEmpty(timeline)) {
            query.append(" ").append("TIMELINE").append(" ").append(timeline);
        }

        ByteBuf startReplicationQuery = ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage(query.toString(), initialChannelHandlerContext.alloc());

        initialChannelHandlerContext.channel().writeAndFlush(startReplicationQuery, initialChannelHandlerContext.voidPromise());
        initialChannelHandlerContext.channel().read();

        keepaliveFuture = initialChannelHandlerContext.channel().eventLoop().scheduleAtFixedRate(
                () -> {
                    ByteBuf response = createStandbyStatusUpdateMessageInCopyDataMessage(initialChannelHandlerContext.alloc(), false);
                    initialChannelHandlerContext.channel().writeAndFlush(response, initialChannelHandlerContext.voidPromise());
                },
                updateIntervalMs,
                updateIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    public void confirmProcessedLsn(PgLogSequenceNumber processed) {
        if (processed.compareTo(lastAppliedLsn) < 0 || processed.compareTo(lastFlushedLsn) < 0) {
            throw new IllegalArgumentException("Confirmed LSN can not be less that already confirmed!");
        }
        lastAppliedLsn = processed;
        lastFlushedLsn = processed;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        cleanUp();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        if (copyDone) {
            cleanUp();
            streamingCompleted.run();
        } else {
            while (message.readerIndex() < message.writerIndex()) {
                if (!readingCopyDataMessage) {
                    boolean readMarkerAndLength = HandlerUtils.readFromBufUntilFilled(internalByteBuf, message, PostgresProtocolGeneralConstants.MESSAGE_MARKER_AND_LENGTH_BYTES_COUNT);
                    if (!readMarkerAndLength) {
                        // failed to read marker and length of message
                        return;
                    }

                    byte marker = internalByteBuf.readByte();
                    int length = internalByteBuf.readInt();

                    switch (marker) {
                        case PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR -> {
                            handleErrorResponseMessage(message, length);
                            return;
                        }
                        case PostgresProtocolGeneralConstants.COPY_BOTH_RESPONSE_START_CHAR -> {
                            handleCopyBothResponseMessage(message, length);
                        }
                        case PostgresProtocolGeneralConstants.COPY_DATA_START_CHAR -> {
                            handelCopyDataMessage(message, length);
                        }
                        case PostgresProtocolGeneralConstants.COPY_DONE_START_CHAR -> {
                            ByteBuf copyDoneMessage = ClientPostgresProtocolMessageEncoder.encodeCopyDoneMessage(ctx.alloc());
                            ctx.channel().writeAndFlush(copyDoneMessage);
                            copyDone = true;
                        }
                        default ->
                                throw new RuntimeException("Unknown message '" + (char) marker + "' during streaming replication! Expected only 'E', 'W' or 'd'!");
                    }
                } else {
                    switch (currentCopyDataMessageStartByte) {
                        case FAKE_COPY_DATA_MESSAGE_START_BYTE -> {
                            // not yet defined CopyData message type
                            currentCopyDataMessageStartByte = message.readByte();
                            leftToReadFromCopyData = leftToReadFromCopyData - 1;
                        }
                        case PostgresProtocolStreamingReplicationConstants.X_LOG_DATA_MESSAGE_START_CHAR -> {
                            handleXLogDataMessage(message, ctx);
                        }
                        case PostgresProtocolStreamingReplicationConstants.PRIMARY_KEEPALIVE_MESSAGE_START_CHAR -> {
                            handlePrimaryKeepalive(message, ctx);
                        }
                        default ->
                                throw new RuntimeException("Unknown message '" + (char) currentCopyDataMessageStartByte + "' in streaming replication copy data sub-protocol!");
                    }
                }
            }
        }

        ctx.channel().read();
        super.channelRead(ctx, msg);
    }

    private void handleErrorResponseMessage(ByteBuf message, int length) {
        // need to read full message
        boolean readMessage = HandlerUtils.readFromBufUntilFilled(internalByteBuf, message, length - 4);
        if (!readMessage) {
            // failed to read entire message
            // reset index to keep marker and length
            internalByteBuf.readerIndex(0);
            return;
        }

        internalByteBuf.readerIndex(0);
        ErrorResponse errorResponse = ServerPostgresProtocolMessageDecoder.decodeErrorResponse(internalByteBuf);

        if (!replicationStarted) {
            // fail start callback
            startedCallback.accept(
                    ReplicationStartResult
                            .builder()
                            .success(false)
                            .serverError(true)
                            .errorResponse(errorResponse)
                            .build()
            );
        } else {
            handleErrorSafe(
                    ReplicationErrorResult
                            .builder()
                            .serverError(true)
                            .errorResponse(errorResponse)
                            .build()
            );
        }

        // message processed
        internalByteBuf.clear();
        log.error("Received error from remote during streaming replication! Code {}, message {}", errorResponse.getCode(), errorResponse.getMessage());
    }

    private void handleCopyBothResponseMessage(ByteBuf message, int length) {
        replicationStarted = true;
        // need to read full message
        boolean readMessage = HandlerUtils.readFromBufUntilFilled(internalByteBuf, message, length - 4);
        if (!readMessage) {
            // failed to read entire message
            // reset index to keep marker and length
            internalByteBuf.readerIndex(0);
        }
        // just skip message because it's useless
        // message processed
        internalByteBuf.clear();
    }

    private void handelCopyDataMessage(ByteBuf message, int length) {
        currentCopyDataMessageLength = length - PostgresProtocolGeneralConstants.MESSAGE_LENGTH_BYTES_COUNT;
        leftToReadFromCopyData = currentCopyDataMessageLength;
        readingCopyDataMessage = true;
        currentCopyDataMessageStartByte = FAKE_COPY_DATA_MESSAGE_START_BYTE;
        finishedReadingCopyDataMessageStartInfo = false;
        // message processed
        internalByteBuf.clear();
    }

    private void handleXLogDataMessage(ByteBuf message, ChannelHandlerContext ctx) {
        if (!finishedReadingCopyDataMessageStartInfo) {
            // not read preamble
            boolean readPreamble = HandlerUtils.readFromBufUntilFilled(
                    internalByteBuf,
                    message,
                    PostgresProtocolStreamingReplicationConstants.X_LOG_DATA_MESSAGE_PREAMBLE_LENGTH);
            if (!readPreamble) {
                internalByteBuf.readerIndex(0);
                return;
            }

            long walDataStartPoint = internalByteBuf.readLong();
            long currentEndOfWalOnServer = internalByteBuf.readLong();
            long serverTimeInPostgresEpoch = internalByteBuf.readLong();

            // skip marker and preamble
            int xLogDataMessageContentSize = currentCopyDataMessageLength - 1 - PostgresProtocolStreamingReplicationConstants.X_LOG_DATA_MESSAGE_PREAMBLE_LENGTH;

            lastFragmentStartLsn = PgLogSequenceNumber.valueOf(walDataStartPoint);
            lastReceivedLsn = PgLogSequenceNumber.valueOf(walDataStartPoint + xLogDataMessageContentSize);

            finishedReadingCopyDataMessageStartInfo = true;
            leftToReadFromCopyData = leftToReadFromCopyData - PostgresProtocolStreamingReplicationConstants.X_LOG_DATA_MESSAGE_PREAMBLE_LENGTH;
            internalByteBuf.clear();
        }

        // can read WAL data
        int bytesInMessage = message.readableBytes();
        int bytesToRead = Math.min(leftToReadFromCopyData, bytesInMessage);

        byte[] byteArrayBuf = TempFastThreadLocalStorageUtils.getThreadLocalByteArray(bytesToRead);
        message.readBytes(byteArrayBuf, 0, bytesToRead);
        leftToReadFromCopyData = leftToReadFromCopyData - bytesToRead;

        // callback
        walFragmentReceivedCallback.accept(
                WalFragmentReceivedResult
                        .builder()
                        .fragmentStartLsn(lastFragmentStartLsn)
                        .fragment(byteArrayBuf)
                        .fragmentLength(bytesToRead)
                        .build()
        );

        lastFragmentStartLsn = PgLogSequenceNumber.valueOf(lastFragmentStartLsn.asLong() + bytesToRead);

        if (leftToReadFromCopyData == 0) {
            readingCopyDataMessage = false;
            currentCopyDataMessageStartByte = FAKE_COPY_DATA_MESSAGE_START_BYTE;
        }
    }

    private void handlePrimaryKeepalive(ByteBuf message, ChannelHandlerContext ctx) {
        boolean readContent = HandlerUtils.readFromBufUntilFilled(
                internalByteBuf,
                message,
                PostgresProtocolStreamingReplicationConstants.PRIMARY_KEEPALIVE_CONTENT_LENGTH);
        if (!readContent) {
            internalByteBuf.readerIndex(0);
            return;
        }

        long currentEndOfWalOnServer = internalByteBuf.readLong();
        long serverTimeInPostgresEpoch = internalByteBuf.readLong();
        byte replyAsSoonAsPossible = internalByteBuf.readByte();

        leftToReadFromCopyData = leftToReadFromCopyData - PostgresProtocolStreamingReplicationConstants.PRIMARY_KEEPALIVE_CONTENT_LENGTH;
        readingCopyDataMessage = false;
        currentCopyDataMessageStartByte = FAKE_COPY_DATA_MESSAGE_START_BYTE;
        internalByteBuf.clear();

        if (replyAsSoonAsPossible == 1) {
            ByteBuf response = createStandbyStatusUpdateMessageInCopyDataMessage(ctx.alloc(), false);
            ctx.channel().writeAndFlush(response, ctx.voidPromise());
        }
    }

    private ByteBuf createStandbyStatusUpdateMessageInCopyDataMessage(ByteBufAllocator allocator, boolean ping) {
        ByteBuf ret = allocator.buffer(PostgresProtocolStreamingReplicationConstants.STANDBY_STATUS_UPDATE_MESSAGE_LENGTH + PostgresProtocolGeneralConstants.MESSAGE_MARKER_AND_LENGTH_BYTES_COUNT);

        // copy data
        ret.writeByte(PostgresProtocolGeneralConstants.COPY_DATA_START_CHAR);
        ret.writeInt(PostgresProtocolStreamingReplicationConstants.STANDBY_STATUS_UPDATE_MESSAGE_LENGTH + PostgresProtocolGeneralConstants.MESSAGE_LENGTH_BYTES_COUNT);

        // standby update
        ret.writeByte(PostgresProtocolStreamingReplicationConstants.STANDBY_STATUS_UPDATE_MESSAGE_START_CHAR);

        ret.writeLong(lastReceivedLsn.asLong());
        ret.writeLong(lastFlushedLsn.asLong());
        ret.writeLong(lastAppliedLsn.asLong());
        ret.writeLong(PostgresTimeUtils.getNowInPostgresTime());

        if (ping) {
            ret.writeByte(1);
        } else {
            ret.writeByte(0);
        }

        return ret;
    }

    private void handleErrorSafe(ReplicationErrorResult replicationErrorResult) {
        try {
            errorCallback.accept(replicationErrorResult);
            HandlerUtils.closeOnFlush(initialChannelHandlerContext.channel(), ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(initialChannelHandlerContext.alloc()));
        } catch (Exception e) {
            log.error("Error during calling error callback for streaming replication handler!");
        }
    }

    private void cleanUp() {
        if (resourcesFreed.compareAndSet(false, true)) {
            keepaliveFuture.cancel(true);
            internalByteBuf.release();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        HandlerUtils.closeOnFlush(ctx.channel());
        cleanUp();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in physical streaming replication handler!", cause);
        cleanUp();
        handleErrorSafe(
                ReplicationErrorResult
                        .builder()
                        .exception(true)
                        .build()
        );
        HandlerUtils.closeOnFlush(ctx.channel(), ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(ctx.alloc()));
    }
}
