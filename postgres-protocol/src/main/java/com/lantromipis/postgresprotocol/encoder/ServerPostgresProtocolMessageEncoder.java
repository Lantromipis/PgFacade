package com.lantromipis.postgresprotocol.encoder;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ServerPostgresProtocolMessageEncoder {

    private final static byte[] PRECOMPILED_AUTH_OK_RESPONSE_BYTES = {
            PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR,
            0,
            0,
            0,
            8,
            0,
            0,
            0,
            0
    };

    private final static byte[] PRECOMPILED_READY_FOR_QUERY_IDLE_TSX_MESSAGE_BYTES = {
            PostgresProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR,
            0,
            0,
            0,
            5,
            PostgresProtocolGeneralConstants.READY_FOR_QUERY_TRANSACTION_IDLE
    };

    public static ByteBuf createErrorMessage(Map<Byte, String> markerAndItsValueMap) {
        // 4 length bytes + 1 last delimiter
        int length = 5;

        ByteBuf content = Unpooled.buffer();
        for (var entry : markerAndItsValueMap.entrySet()) {
            byte[] valueBytes = entry.getValue().getBytes();

            // field length + 1 byte field marker + 1 byte delimiter
            length += valueBytes.length + 2;

            content.writeByte(entry.getKey());
            content.writeBytes(valueBytes);
            content.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);
        }

        ByteBuf buf = Unpooled.buffer(length + 1);
        buf.writeByte(PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR);
        buf.writeInt(length);
        buf.writeBytes(content);
        buf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);

        return buf;
    }

    public static ByteBuf createEmptyErrorMessage() {
        ByteBuf buf = Unpooled.buffer(6);
        buf.writeByte(PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR);
        // 4 length bytes + 1 code byte
        buf.writeInt(5);
        // from Docs: A code identifying the field type; if zero, this is the message terminator and no string follows.
        buf.writeByte(0);

        return buf;
    }

    // no support fro SCRAM-SHA256-PLUS
    public static ByteBuf createAuthenticationSASLMessage(ByteBufAllocator allocator) {
        byte[] nameOfSaslAuthMechanism = PostgresProtocolAuthenticationMethod.SCRAM_SHA256.getProtocolMethodName().getBytes();
        int length = nameOfSaslAuthMechanism.length;
        // 4 bytes length + terminator + last byte
        length += 10;

        ByteBuf buf = allocator.buffer(length + 1);

        buf.writeByte(PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR);
        buf.writeInt(length);
        buf.writeInt(PostgresProtocolAuthenticationMethod.SCRAM_SHA256.getProtocolMethodMarker());
        buf.writeBytes(nameOfSaslAuthMechanism);
        //terminator
        buf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);
        //end byte
        buf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);

        return buf;
    }

    public static ByteBuf createAuthenticationSaslContinueMessage(String message, ByteBufAllocator allocator) {
        byte[] messageBytes = message.getBytes();

        //4 bytes length + 4 bytes marker
        int length = 8 + messageBytes.length;

        ByteBuf buf = allocator.buffer(length + 1);

        buf.writeByte(PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR);
        buf.writeInt(length);
        buf.writeInt(PostgresProtocolGeneralConstants.SASL_AUTH_CHALLENGE_MARKER);
        buf.writeBytes(messageBytes);

        return buf;
    }

    public static ByteBuf createAuthenticationOkMessage(ByteBufAllocator allocator) {
        ByteBuf buf = allocator.buffer(9);

        buf.writeBytes(PRECOMPILED_AUTH_OK_RESPONSE_BYTES);

        return buf;
    }

    public static ByteBuf createAuthenticationSASLFinalMessageWithAuthOk(String message, ByteBufAllocator allocator) {
        byte[] messageBytes = message.getBytes();

        //4 bytes length + 4 bytes marker
        int length = 8 + messageBytes.length;

        // 1 byte for marker, 9 fro AuthOk message
        ByteBuf buf = allocator.buffer(length + 10);

        buf.writeByte(PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR);
        buf.writeInt(length);
        buf.writeInt(PostgresProtocolGeneralConstants.SASL_AUTH_COMPLETED_MARKER);
        buf.writeBytes(messageBytes);
        buf.writeBytes(PRECOMPILED_AUTH_OK_RESPONSE_BYTES);

        return buf;
    }

    public static ByteBuf encodeParameterStatusMessage(String parameterName, String parameterValue) {
        byte[] parameterNameBytes = parameterName.getBytes();
        byte[] parameterValueBytes = parameterValue.getBytes();

        // length 4 bytes + 2 delimiter byte
        int length = 6 + parameterNameBytes.length + parameterValueBytes.length;

        ByteBuf buf = Unpooled.buffer(length + 1);

        buf.writeByte(PostgresProtocolGeneralConstants.PARAMETER_STATUS_MESSAGE_START_CHAR);
        buf.writeInt(length);
        buf.writeBytes(parameterNameBytes);
        buf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);
        buf.writeBytes(parameterValueBytes);
        buf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);

        return buf;
    }

    public static ByteBuf encodeReadyForQueryWithIdleTsxMessage(ByteBufAllocator allocator) {
        ByteBuf buf = allocator.buffer(6);

        buf.writeBytes(PRECOMPILED_READY_FOR_QUERY_IDLE_TSX_MESSAGE_BYTES);

        return buf;
    }

    public static byte[] encodeReadyForQueryWithIdleTsxMessage() {
        return PRECOMPILED_READY_FOR_QUERY_IDLE_TSX_MESSAGE_BYTES;
    }
}
