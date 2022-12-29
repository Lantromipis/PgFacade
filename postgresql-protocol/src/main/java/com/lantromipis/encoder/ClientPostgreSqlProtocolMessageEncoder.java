package com.lantromipis.encoder;

import com.lantromipis.constant.PostgreSQLProtocolGeneralConstants;
import com.lantromipis.model.SaslInitialResponse;
import com.lantromipis.model.SaslResponse;
import com.lantromipis.model.StartupMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ClientPostgreSqlProtocolMessageEncoder {
    public static ByteBuf encodeClientStartupMessage(StartupMessage startupMessage) {
        ByteBuf buf = Unpooled.buffer();

        //4 bytes length + 4 bytes version + 1 byte final delimiter
        int length = 9;

        ByteBuf paramsBuf = Unpooled.buffer();

        for (var e : startupMessage.getParameters().entrySet()) {
            byte[] paramNameBytes = e.getKey().getBytes();
            byte[] paramValueBytes = e.getValue().getBytes();

            paramsBuf.writeBytes(paramNameBytes);
            paramsBuf.writeByte(PostgreSQLProtocolGeneralConstants.DELIMITER_BYTE);
            paramsBuf.writeBytes(paramValueBytes);
            paramsBuf.writeByte(PostgreSQLProtocolGeneralConstants.DELIMITER_BYTE);

            length += paramNameBytes.length;
            length += paramValueBytes.length;
            //delimiter
            length += 2;
        }

        paramsBuf.writeByte(PostgreSQLProtocolGeneralConstants.DELIMITER_BYTE);

        buf.writeInt(length);
        buf.writeShort(startupMessage.getMajorVersion());
        buf.writeShort(startupMessage.getMinorVersion());
        buf.writeBytes(paramsBuf);

        return buf;
    }

    public static ByteBuf encodeSaslInitialResponseMessage(SaslInitialResponse saslInitialResponse) {
        ByteBuf buf = Unpooled.buffer();

        byte[] mechanismNameBytes = saslInitialResponse.getNameOfSaslAuthMechanism().getBytes();
        byte[] saslSpecificData = saslInitialResponse.getSaslMechanismSpecificData().getBytes();

        //length (int32) + 1 delimiter byte + length of sasl data (int32)
        int length = 9 + mechanismNameBytes.length + saslSpecificData.length;

        buf.writeByte(PostgreSQLProtocolGeneralConstants.CLIENT_PASSWORD_RESPONSE_START_CHAR);
        buf.writeInt(length);
        buf.writeBytes(mechanismNameBytes);
        buf.writeByte(PostgreSQLProtocolGeneralConstants.DELIMITER_BYTE);
        buf.writeInt(saslSpecificData.length);
        buf.writeBytes(saslSpecificData);

        return buf;
    }

    public static ByteBuf encodeSaslResponseMessage(SaslResponse saslResponse) {
        ByteBuf buf = Unpooled.buffer();

        byte[] saslSpecificData = saslResponse.getSaslMechanismSpecificData().getBytes();

        //length (int32)
        int length = 4 + saslSpecificData.length;

        buf.writeByte(PostgreSQLProtocolGeneralConstants.CLIENT_PASSWORD_RESPONSE_START_CHAR);
        buf.writeInt(length);
        buf.writeBytes(saslSpecificData);

        return buf;
    }
}
