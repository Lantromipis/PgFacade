package com.lantromipis.postgresprotocol.encoder;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.SaslInitialResponse;
import com.lantromipis.postgresprotocol.model.SaslResponse;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ClientPostgresProtocolMessageEncoder {

    public static ByteBuf encodeClientTerminateMessage() {
        ByteBuf buf = Unpooled.buffer(5);

        buf.writeByte(PostgresProtocolGeneralConstants.CLIENT_TERMINATION_MESSAGE_START_CHAR);
        buf.writeInt(4);

        return buf;
    }

    public static ByteBuf encodeClientStartupMessage(StartupMessage startupMessage) {
        //4 bytes length + 4 bytes version + 1 byte final delimiter
        int length = 9;

        ByteBuf paramsBuf = Unpooled.buffer();

        for (var e : startupMessage.getParameters().entrySet()) {
            byte[] paramNameBytes = e.getKey().getBytes();
            byte[] paramValueBytes = e.getValue().getBytes();

            paramsBuf.writeBytes(paramNameBytes);
            paramsBuf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);
            paramsBuf.writeBytes(paramValueBytes);
            paramsBuf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);

            length += paramNameBytes.length;
            length += paramValueBytes.length;
            //delimiter
            length += 2;
        }

        paramsBuf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);

        ByteBuf buf = Unpooled.buffer(length + 1);

        buf.writeInt(length);
        buf.writeShort(startupMessage.getMajorVersion());
        buf.writeShort(startupMessage.getMinorVersion());
        buf.writeBytes(paramsBuf);

        return buf;
    }

    public static ByteBuf encodeSaslInitialResponseMessage(SaslInitialResponse saslInitialResponse) {
        byte[] mechanismNameBytes = saslInitialResponse.getNameOfSaslAuthMechanism().getBytes();
        byte[] saslSpecificData = saslInitialResponse.getSaslMechanismSpecificData().getBytes();

        //length (int32) + 1 delimiter byte + length of sasl data (int32)
        int length = 9 + mechanismNameBytes.length + saslSpecificData.length;

        ByteBuf buf = Unpooled.buffer(length + 1);

        buf.writeByte(PostgresProtocolGeneralConstants.CLIENT_PASSWORD_RESPONSE_START_CHAR);
        buf.writeInt(length);
        buf.writeBytes(mechanismNameBytes);
        buf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);
        buf.writeInt(saslSpecificData.length);
        buf.writeBytes(saslSpecificData);

        return buf;
    }

    public static ByteBuf encodeSaslResponseMessage(SaslResponse saslResponse) {
        byte[] saslSpecificData = saslResponse.getSaslMechanismSpecificData().getBytes();

        //length (int32)
        int length = 4 + saslSpecificData.length;

        ByteBuf buf = Unpooled.buffer(length + 1);

        buf.writeByte(PostgresProtocolGeneralConstants.CLIENT_PASSWORD_RESPONSE_START_CHAR);
        buf.writeInt(length);
        buf.writeBytes(saslSpecificData);

        return buf;
    }
}
