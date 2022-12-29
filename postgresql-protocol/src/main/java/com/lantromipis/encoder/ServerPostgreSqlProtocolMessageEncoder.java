package com.lantromipis.encoder;

import com.lantromipis.constant.PostgreSQLProtocolGeneralConstants;
import com.lantromipis.model.AuthenticationMethod;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class ServerPostgreSqlProtocolMessageEncoder {

    public static ByteBuf createErrorMessage(byte errorFieldCode, String errorFieldValue) {
        ByteBuf buf = Unpooled.buffer();

        if (errorFieldCode == 0 || StringUtils.isEmpty(errorFieldValue)) {
            buf.writeByte(PostgreSQLProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR);
            //1 start byte + 4 length bytes + 1 code byte
            buf.writeInt(6);
            buf.writeByte(0);
        } else {
            byte[] valueBytes = errorFieldValue.getBytes();

            //value length + 1 start byte + 4 length bytes + 1 code byte
            int length = valueBytes.length + 6;

            buf.writeByte(PostgreSQLProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR);
            buf.writeInt(length);
            buf.writeByte(errorFieldCode);
            buf.writeBytes(valueBytes);
        }

        return buf;
    }

    public static ByteBuf createEmptyErrorMessage() {
        return createErrorMessage(PostgreSQLProtocolGeneralConstants.DELIMITER_BYTE, null);
    }

    public static ByteBuf createAuthRequestMessage(AuthenticationMethod authenticationMethod) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(PostgreSQLProtocolGeneralConstants.AUTH_REQUEST_START_CHAR);

        //4 bytes length
        int length = 4;

        byte[] nameOfSaslAuthMechanism = authenticationMethod.getProtocolMethodName().getBytes();
        length += nameOfSaslAuthMechanism.length;
        //4 bytes marker + terminator + last byte
        length += 6;

        buf.writeInt(length);
        buf.writeInt(authenticationMethod.getProtocolMethodMarker());
        buf.writeBytes(nameOfSaslAuthMechanism);
        //terminator
        buf.writeByte(PostgreSQLProtocolGeneralConstants.DELIMITER_BYTE);
        //end byte
        buf.writeByte(PostgreSQLProtocolGeneralConstants.DELIMITER_BYTE);

        return buf;
    }

    public static ByteBuf createAuthenticationSaslContinueMessage(String message) {
        ByteBuf buf = Unpooled.buffer();

        byte[] messageBytes = message.getBytes();

        //4 bytes length + 4 bytes marker
        int length = 8 + messageBytes.length;

        buf.writeByte(PostgreSQLProtocolGeneralConstants.AUTH_REQUEST_START_CHAR);
        buf.writeInt(length);
        buf.writeInt(PostgreSQLProtocolGeneralConstants.SASL_AUTH_CHALLENGE_MARKER);
        buf.writeBytes(messageBytes);

        return buf;
    }

    public static ByteBuf createAuthenticationOkMessage() {
        ByteBuf buf = Unpooled.buffer();

        buf.writeByte(PostgreSQLProtocolGeneralConstants.AUTH_REQUEST_START_CHAR);
        //4 bytes length + 4 bytes marker
        buf.writeInt(8);
        buf.writeInt(PostgreSQLProtocolGeneralConstants.AUTH_OK);

        return buf;
    }

    public static ByteBuf createAuthenticationSASLFinalMessage(String message) {
        ByteBuf buf = Unpooled.buffer();

        byte[] messageBytes = message.getBytes();

        //4 bytes length + 4 bytes marker
        int length = 8 + messageBytes.length;

        buf.writeByte(PostgreSQLProtocolGeneralConstants.AUTH_REQUEST_START_CHAR);
        buf.writeInt(length);
        buf.writeInt(PostgreSQLProtocolGeneralConstants.SASL_AUTH_COMPLETED_MARKER);
        buf.writeBytes(messageBytes);

        return buf;
    }

    public static ByteBuf encodeParameterStatusMessage(String parameterName, String parameterValue) {
        ByteBuf buf = Unpooled.buffer();

        byte[] parameterNameBytes = parameterName.getBytes();
        byte[] parameterValueBytes = parameterValue.getBytes();

        //length (int32)
        int length = 4 + parameterNameBytes.length + parameterValueBytes.length;

        buf.writeByte(PostgreSQLProtocolGeneralConstants.PARAMETER_STATUS_MESSAGE_START_CHAR);
        buf.writeInt(length);
        buf.writeBytes(parameterNameBytes);
        buf.writeByte(PostgreSQLProtocolGeneralConstants.DELIMITER_BYTE);
        buf.writeBytes(parameterValueBytes);

        return buf;
    }

    public static ByteBuf encodeReadyForQueryMessage() {
        ByteBuf buf = Unpooled.buffer();

        //constant
        int length = 5;

        buf.writeByte(PostgreSQLProtocolGeneralConstants.READY_FOR_QUERY_MESSAGE_START_CHAR);
        buf.writeInt(length);
        //TODO maybe add other
        buf.writeByte(PostgreSQLProtocolGeneralConstants.READY_FOR_QUERY_TRANSACTION_IDLE);

        return buf;
    }
}
