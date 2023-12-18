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

import java.util.function.Consumer;

@Slf4j
public class PgStreamingReplicationHandler extends AbstractPgFrontendChannelHandler {

    private static final byte FAKE_COPY_DATA_MESSAGE_START_BYTE = 0;

    private boolean replicationStarted = false;
    private Consumer<ReplicationStartResult> startedCallback;

    private boolean readingCopyDataMessage = false;
    private byte currentCopyDataMessageStartByte = FAKE_COPY_DATA_MESSAGE_START_BYTE;
    private int currentCopyDataMessageLength = 0;
    private int leftToReadFromCopyData = 0;
    private boolean finishedReadingCopyDataMessageStartInfo = false;

    private PgLogSequenceNumber lastServerLSN = PgLogSequenceNumber.INVALID_LSN;
    private PgLogSequenceNumber lastReceivedLsn = PgLogSequenceNumber.INVALID_LSN;
    private PgLogSequenceNumber lastFlushedLsn = PgLogSequenceNumber.INVALID_LSN;
    private PgLogSequenceNumber lastAppliedLsn = PgLogSequenceNumber.INVALID_LSN;


    private ByteBuf internalByteBuf;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReplicationStartResult {
        private boolean success;
        private boolean serverError;
        private ErrorResponse errorResponse;
    }

    public void startPhysicalReplication(final String slotName,
                                         final String startLsn,
                                         final String timeline,
                                         final Consumer<ReplicationStartResult> startedCallback) {
        this.startedCallback = startedCallback;

        StringBuilder query = new StringBuilder("START_REPLICATION");

        if (StringUtils.isNotEmpty(slotName)) {
            query.append(" ").append("SLOT").append(" ").append(slotName);
        }

        query.append(" ").append("PHYSICAL");
        query.append(" ").append(startLsn);
        lastReceivedLsn = PgLogSequenceNumber.valueOf(startLsn);

        if (StringUtils.isNotEmpty(timeline)) {
            query.append(" ").append("TIMELINE").append(" ").append(slotName);
        }

        ByteBuf startReplicationQuery = ClientPostgresProtocolMessageEncoder.encodeSimpleQueryMessage(query.toString(), initialChannelHandlerContext.alloc());

        initialChannelHandlerContext.channel().writeAndFlush(startReplicationQuery, initialChannelHandlerContext.voidPromise());
        initialChannelHandlerContext.channel().read();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
        internalByteBuf = ctx.alloc().buffer(2048);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        internalByteBuf.release();
        internalByteBuf = null;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        if (!readingCopyDataMessage) {
            loop:
            while (message.readerIndex() < message.writerIndex()) {
                boolean readMarkerAndLength = HandlerUtils.readFromBufUntilFilled(internalByteBuf, message, PostgresProtocolGeneralConstants.MESSAGE_MARKER_AND_LENGTH_BYTES_COUNT);
                if (!readMarkerAndLength) {
                    // failed to read marker and length of message
                    return;
                }

                byte marker = internalByteBuf.readByte();
                int length = internalByteBuf.readInt();

                switch (marker) {
                    case PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR -> {
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
                            return;
                        } else {

                        }
                    }
                    case PostgresProtocolGeneralConstants.COPY_BOTH_RESPONSE_START_CHAR -> {
                        replicationStarted = true;
                        // need to read full message
                        boolean readMessage = HandlerUtils.readFromBufUntilFilled(internalByteBuf, message, length - 4);
                        if (!readMessage) {
                            // failed to read entire message
                            // reset index to keep marker and length
                            internalByteBuf.readerIndex(0);
                            return;
                        }
                        // just skip message because it's useless
                    }
                    case PostgresProtocolGeneralConstants.COPY_DATA_START_CHAR -> {
                        currentCopyDataMessageLength = length - PostgresProtocolGeneralConstants.MESSAGE_LENGTH_BYTES_COUNT;
                        leftToReadFromCopyData = currentCopyDataMessageLength;
                        readingCopyDataMessage = true;
                        currentCopyDataMessageStartByte = FAKE_COPY_DATA_MESSAGE_START_BYTE;
                        finishedReadingCopyDataMessageStartInfo = false;
                        // clear buf
                        internalByteBuf.clear();
                        // break loop
                        break loop;
                    }
                    default ->
                            throw new RuntimeException("Unknown message '" + (char) marker + "' during streaming replication! Expected only 'E', 'W' or 'd'!");
                }

                // message processed
                internalByteBuf.clear();
            }
        }

        // read CopyData message until packet not ended
        while (message.readerIndex() < message.writerIndex()) {
            switch (currentCopyDataMessageStartByte) {
                case FAKE_COPY_DATA_MESSAGE_START_BYTE -> {
                    // not yet defined CopyData message type
                    currentCopyDataMessageStartByte = message.readByte();
                    leftToReadFromCopyData = leftToReadFromCopyData - 1;
                }
                case PostgresProtocolStreamingReplicationConstants.X_LOG_DATA_MESSAGE_START_CHAR -> {
                    log.info("Received XLogData");
                    handleXLogDataMessage(message, ctx);
                }
                case PostgresProtocolStreamingReplicationConstants.PRIMARY_KEEPALIVE_MESSAGE_START_CHAR -> {
                    log.info("Received Keepalive");
                    handlePrimaryKeepalive(message, ctx);
                }
                default ->
                        throw new RuntimeException("Unknown message '" + (char) currentCopyDataMessageStartByte + "' in streaming replication copy data sub-protocol!");
            }
        }

        ctx.channel().read();
        super.channelRead(ctx, msg);
    }

    private void handleErrorResponseMessage(ByteBuf message, ChannelHandlerContext ctx) {

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

            lastServerLSN = PgLogSequenceNumber.valueOf(currentEndOfWalOnServer);
            // skip marker and preamble
            int xLogDataMessageContentSize = currentCopyDataMessageLength - 1 - PostgresProtocolStreamingReplicationConstants.X_LOG_DATA_MESSAGE_PREAMBLE_LENGTH;
            lastReceivedLsn = PgLogSequenceNumber.valueOf(currentEndOfWalOnServer + xLogDataMessageContentSize);
            lastAppliedLsn = lastReceivedLsn;
            lastFlushedLsn = lastAppliedLsn;

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
        log.info("Read {} wal bytes and left to read is {} with message size {} and readable bytes {}", bytesToRead, leftToReadFromCopyData, message.writerIndex(), message.readableBytes());
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

        lastServerLSN = PgLogSequenceNumber.valueOf(currentEndOfWalOnServer);

        leftToReadFromCopyData = leftToReadFromCopyData - PostgresProtocolStreamingReplicationConstants.PRIMARY_KEEPALIVE_CONTENT_LENGTH;
        readingCopyDataMessage = false;
        currentCopyDataMessageStartByte = FAKE_COPY_DATA_MESSAGE_START_BYTE;
        internalByteBuf.clear();

        // TODO check if need to reply
        ByteBuf response = createStandbyStatusUpdateMessage(ctx.alloc(), false);
        ctx.channel().writeAndFlush(response, ctx.voidPromise());
    }

    private ByteBuf createStandbyStatusUpdateMessage(ByteBufAllocator allocator, boolean ping) {
        ByteBuf ret = allocator.buffer(PostgresProtocolStreamingReplicationConstants.STANDBY_STATUS_UPDATE_MESSAGE_LENGTH);

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

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        HandlerUtils.closeOnFlush(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in PgChannelSimpleQueryExecutorHandler ", cause);

        HandlerUtils.closeOnFlush(ctx.channel(), ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(ctx.alloc()));
    }


}
