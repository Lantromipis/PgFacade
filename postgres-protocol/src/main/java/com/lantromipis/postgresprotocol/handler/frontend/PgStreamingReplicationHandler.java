package com.lantromipis.postgresprotocol.handler.frontend;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolStreamingReplicationConstants;
import com.lantromipis.postgresprotocol.decoder.ServerPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.PgResultSet;
import com.lantromipis.postgresprotocol.model.internal.PgMessageInfo;
import com.lantromipis.postgresprotocol.model.protocol.ErrorResponse;
import com.lantromipis.postgresprotocol.utils.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
public class PgStreamingReplicationHandler extends AbstractPgFrontendChannelHandler {

    private static final byte FAKE_COPY_DATA_MESSAGE_START_BYTE = 0;

    private AtomicBoolean replicationRunning;
    private Consumer<ReplicationStartResult> startedCallback;
    private Consumer<ReplicationErrorResult> errorCallback;
    private Consumer<WalFragmentReceivedResult> walFragmentReceivedCallback;
    private Consumer<StreamingCompletedResult> streamingCompletedCallback;
    private Runnable serverTimeout;

    private boolean readingCopyDataMessage = false;
    private byte currentCopyDataMessageStartByte = FAKE_COPY_DATA_MESSAGE_START_BYTE;
    private int currentCopyDataMessageLength = 0;
    private int leftToReadFromCopyData = 0;
    private boolean finishedReadingCopyDataMessageStartInfo = false;

    private long lastFragmentStartLsn = LogSequenceNumberUtils.INVALID_LSN;
    private long lastReceivedLsn = LogSequenceNumberUtils.INVALID_LSN;
    private long lastFlushedLsn = LogSequenceNumberUtils.INVALID_LSN;
    private long lastAppliedLsn = LogSequenceNumberUtils.INVALID_LSN;

    private long timeline;
    private String slotName;
    private long updateIntervalMs;
    private ScheduledFuture<?> keepaliveFuture;

    private ByteBuf internalByteBuf;
    private Deque<PgMessageInfo> messageInfos;
    private ByteBuf leftovers;

    private AtomicBoolean resourcesFreed;
    private boolean streamingCompleted;
    private AtomicBoolean active;
    private AtomicBoolean inCopyDataMode;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReplicationStartResult {
        private StartResultStatus status;
        private ErrorResponse errorResponse;
        private Throwable throwable;

        public enum StartResultStatus {
            SUCCESS,
            SUCCESS_END_OF_TIMELINE,
            SERVER_ERROR,
            CLIENT_ERROR
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReplicationErrorResult {
        private ErrorType errorType;
        private ErrorResponse errorResponse;
        private Throwable throwable;

        public enum ErrorType {
            SERVER_ERROR,
            CLIENT_ERROR
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WalFragmentReceivedResult {
        private long fragmentStartLsn;
        private long fragmentEndLsn;
        private byte[] fragment;
        private int fragmentLength;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StreamingCompletedResult {
        private String nextTimeline;
        private String nextTimelineStartLsn;
    }

    public PgStreamingReplicationHandler() {
        resourcesFreed = new AtomicBoolean(true);
        messageInfos = new ArrayDeque<>();
        replicationRunning = new AtomicBoolean(false);
        active = new AtomicBoolean(false);
    }

    public void startPhysicalReplication(final String slotName,
                                         final long startLsn,
                                         final long timeline,
                                         final long updateIntervalMs,
                                         final Consumer<ReplicationStartResult> startedCallback,
                                         final Consumer<ReplicationErrorResult> errorCallback,
                                         final Consumer<WalFragmentReceivedResult> walFragmentReceivedCallback,
                                         final Consumer<StreamingCompletedResult> streamingCompleted,
                                         final Runnable serverTimeout) {
        cleanUp();

        this.internalByteBuf = initialChannelHandlerContext.alloc().buffer(2048);
        this.resourcesFreed = new AtomicBoolean(false);
        this.replicationRunning = new AtomicBoolean(false);
        this.inCopyDataMode = new AtomicBoolean(false);

        this.startedCallback = startedCallback;
        this.errorCallback = errorCallback;
        this.walFragmentReceivedCallback = walFragmentReceivedCallback;
        this.timeline = timeline;
        this.slotName = slotName;
        this.updateIntervalMs = updateIntervalMs;
        this.streamingCompletedCallback = streamingCompleted;
        this.serverTimeout = serverTimeout;
        this.streamingCompleted = false;

        StringBuilder query = new StringBuilder("START_REPLICATION");

        if (StringUtils.isNotEmpty(slotName)) {
            query.append(" ").append("SLOT").append(" ").append(slotName);
        }

        query.append(" ").append("PHYSICAL");
        query.append(" ").append(LogSequenceNumberUtils.lsnToString(startLsn));
        query.append(" ").append("TIMELINE").append(" ").append(LogSequenceNumberUtils.timelineToStr(timeline));

        lastReceivedLsn = startLsn;
        lastAppliedLsn = lastReceivedLsn;
        lastFlushedLsn = lastReceivedLsn;

        ByteBuf startReplicationQuery = ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage(query.toString(), initialChannelHandlerContext.alloc());

        initialChannelHandlerContext.channel().writeAndFlush(startReplicationQuery, initialChannelHandlerContext.voidPromise());
        initialChannelHandlerContext.channel().read();
        this.active.set(true);

        keepaliveFuture = initialChannelHandlerContext.channel().eventLoop().scheduleAtFixedRate(
                () -> {
                    ByteBuf response = createStandbyStatusUpdateMessageInCopyDataMessage(initialChannelHandlerContext.alloc(), false);
                    initialChannelHandlerContext.channel().writeAndFlush(response, initialChannelHandlerContext.voidPromise());
                },
                updateIntervalMs,
                updateIntervalMs,
                TimeUnit.MILLISECONDS
        );

        DecoderUtils.freeMessageInfos(messageInfos);
    }

    public void confirmProcessedLsn(long processed) {
        if (LogSequenceNumberUtils.compareTwoLsn(lastAppliedLsn, processed) > 0 || LogSequenceNumberUtils.compareTwoLsn(lastFlushedLsn, processed) > 0) {
            return;
        }
        lastAppliedLsn = processed;
        lastFlushedLsn = processed;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        cleanUp();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        ByteBuf message = (ByteBuf) msg;

        if (streamingCompleted) {
            // ensure no code after this method, because it can fail if there is not enough data in ByteBuf
            handleMessageForCompletedStreaming(message, ctx);
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
                            streamingCompleted = true;
                            inCopyDataMode.set(false);
                        }
                        case PostgresProtocolGeneralConstants.ROW_DESCRIPTION_START_CHAR -> {
                            if (inCopyDataMode.get()) {
                                throw new RuntimeException("Did not expect message of type '" + (char) marker + "' during streaming replication in COPY DATA mode! Expected only 'E', 'd' or 'c'!");
                            }
                            // means that server skipped COPY mode because requested WAL is at the end of timeline
                            // need to make same actions as when server sent COPY DONE message
                            streamingCompleted = true;

                            // mark replication as started with special SUCCESS status
                            startedCallback.accept(
                                    ReplicationStartResult
                                            .builder()
                                            .status(ReplicationStartResult.StartResultStatus.SUCCESS_END_OF_TIMELINE)
                                            .build()
                            );

                            // marker and length already read
                            // not sure if they were in same message, so make them as leftovers
                            // even if they were in same message, readerIndex of message is 5, so splitMessages will work
                            leftovers = ctx.alloc().buffer(PostgresProtocolGeneralConstants.MESSAGE_MARKER_AND_LENGTH_BYTES_COUNT);
                            leftovers.writeByte(marker);
                            leftovers.writeInt(length);

                            handleMessageForCompletedStreaming(message, ctx);
                            // do not read more messages!
                            return;
                        }
                        default -> {
                            ctx.pipeline().addFirst(new LoggingHandler(this.getClass(), LogLevel.INFO));
                            throw new RuntimeException("Unknown message '" + (char) marker + "' during streaming replication! Expected only 'E', 'W', 'c', 'd' or 'T'!");
                        }
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
            ctx.channel().read();
        }
    }

    private void handleMessageForCompletedStreaming(ByteBuf message, ChannelHandlerContext ctx) {
        ByteBuf newLeftovers = DecoderUtils.splitToMessages(leftovers, message, messageInfos, ctx.alloc());
        if (leftovers != null) {
            leftovers.release();
        }
        leftovers = newLeftovers;

        // if not found ready for query, retry
        if (!DecoderUtils.containsMessageOfTypeReversed(messageInfos, PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR)) {
            ctx.channel().read();
            return;
        }

        PgResultSet resultSet = DecoderUtils.extractResultSetFromMessages(messageInfos);
        PgResultSet.PgRow row = resultSet.getRow(0);

        String nextTimeline = new String(row.getCellValueByIdx(0));
        String nextTimelineStartLsn = new String(row.getCellValueByIdx(1));

        streamingCompletedCallback.accept(
                StreamingCompletedResult
                        .builder()
                        .nextTimeline(nextTimeline)
                        .nextTimelineStartLsn(nextTimelineStartLsn)
                        .build()
        );

        cleanUp();
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

        if (!replicationRunning.get()) {
            // fail start callback
            startedCallback.accept(
                    ReplicationStartResult
                            .builder()
                            .status(ReplicationStartResult.StartResultStatus.SERVER_ERROR)
                            .errorResponse(errorResponse)
                            .build()
            );
        } else {
            errorCallback.accept(
                    ReplicationErrorResult
                            .builder()
                            .errorType(ReplicationErrorResult.ErrorType.SERVER_ERROR)
                            .errorResponse(errorResponse)
                            .build()
            );
        }

        active.set(false);
        // message processed
        internalByteBuf.clear();
    }

    private void handleCopyBothResponseMessage(ByteBuf message, int length) {
        // need to read full message
        boolean readMessage = HandlerUtils.readFromBufUntilFilled(internalByteBuf, message, length - 4);
        if (!readMessage) {
            // failed to read entire message
            // reset index to keep marker and length
            internalByteBuf.readerIndex(0);
            return;
        }
        // just skip message because it's useless
        // message processed
        internalByteBuf.clear();
        inCopyDataMode.set(true);
    }

    private void handelCopyDataMessage(ByteBuf message, int length) {
        currentCopyDataMessageLength = length - PostgresProtocolGeneralConstants.MESSAGE_LENGTH_BYTES_COUNT;
        leftToReadFromCopyData = currentCopyDataMessageLength;
        readingCopyDataMessage = true;
        currentCopyDataMessageStartByte = FAKE_COPY_DATA_MESSAGE_START_BYTE;
        finishedReadingCopyDataMessageStartInfo = false;

        // Replication started normally when first CopyData received
        if (replicationRunning.compareAndSet(false, true)) {
            startedCallback.accept(
                    ReplicationStartResult
                            .builder()
                            .status(ReplicationStartResult.StartResultStatus.SUCCESS)
                            .build()
            );
        }

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

            lastFragmentStartLsn = walDataStartPoint;
            lastReceivedLsn = walDataStartPoint + xLogDataMessageContentSize;

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

        long fragmentEndLsn = lastFragmentStartLsn + bytesToRead;

        // callback
        walFragmentReceivedCallback.accept(
                WalFragmentReceivedResult
                        .builder()
                        .fragmentStartLsn(lastFragmentStartLsn)
                        .fragment(byteArrayBuf)
                        .fragmentLength(bytesToRead)
                        .fragmentEndLsn(fragmentEndLsn)
                        .build()
        );

        lastFragmentStartLsn = fragmentEndLsn;

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

        ret.writeLong(lastReceivedLsn);
        ret.writeLong(lastFlushedLsn);
        ret.writeLong(lastAppliedLsn);
        ret.writeLong(PostgresTimeUtils.getNowInPostgresTime());

        if (ping) {
            ret.writeByte(1);
        } else {
            ret.writeByte(0);
        }

        return ret;
    }

    private void cleanUp() {
        if (resourcesFreed.compareAndSet(false, true)) {
            active.set(false);
            keepaliveFuture.cancel(true);
            internalByteBuf.release();
            DecoderUtils.freeMessageInfos(messageInfos);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (active.compareAndSet(true, false)) {
            serverTimeout.run();
        }
        cleanUp();
        HandlerUtils.closeOnFlush(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            if (!replicationRunning.get()) {
                // fail start callback
                startedCallback.accept(
                        ReplicationStartResult
                                .builder()
                                .status(ReplicationStartResult.StartResultStatus.CLIENT_ERROR)
                                .throwable(cause)
                                .build()
                );
            } else {
                errorCallback.accept(
                        ReplicationErrorResult
                                .builder()
                                .errorType(ReplicationErrorResult.ErrorType.CLIENT_ERROR)
                                .throwable(cause)
                                .build()
                );
            }
        } catch (Throwable t) {
            log.error("Error in streaming replication handler!", cause);
        }

        cleanUp();
        HandlerUtils.closeOnFlush(ctx.channel(), ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(ctx.alloc()));
    }
}
