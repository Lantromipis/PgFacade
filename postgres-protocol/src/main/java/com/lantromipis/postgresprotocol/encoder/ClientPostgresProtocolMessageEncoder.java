package com.lantromipis.postgresprotocol.encoder;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.protocol.SaslInitialResponse;
import com.lantromipis.postgresprotocol.model.protocol.SaslResponse;
import com.lantromipis.postgresprotocol.model.protocol.StartupMessage;
import com.lantromipis.postgresprotocol.utils.TempFastThreadLocalStorageUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class ClientPostgresProtocolMessageEncoder {

    public static ByteBuf encodeSimpleQueryMessage(String sqlStatement, ByteBufAllocator allocator) {
        byte[] sqlStatementBytes = sqlStatement.getBytes();

        // 4 bytes length + 1 delimiter byte
        int length = 5 + sqlStatementBytes.length;

        ByteBuf buf = allocator.buffer(length + 1);

        buf.writeByte(PostgresProtocolGeneralConstants.QUERY_MESSAGE_START_BYTE);
        buf.writeInt(length);

        buf.writeBytes(sqlStatementBytes);
        buf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);

        return buf;
    }

    public static ByteBuf encodeClientTerminateMessage(ByteBufAllocator allocator) {
        ByteBuf buf = allocator.buffer(5);

        buf.writeByte(PostgresProtocolGeneralConstants.CLIENT_TERMINATION_MESSAGE_START_CHAR);
        buf.writeInt(4);

        return buf;
    }

    public static ByteBuf encodeClientStartupMessage(StartupMessage startupMessage, ByteBufAllocator allocator) {
        //4 bytes length + 4 bytes version + 1 byte final delimiter
        int length = 9;

        byte[] paramsBuf = TempFastThreadLocalStorageUtils.getThreadLocalByteArray();
        int paramsIdx = 0;

        for (var e : startupMessage.getParameters().entrySet()) {
            byte[] paramNameBytes = e.getKey().getBytes();
            byte[] paramValueBytes = e.getValue().getBytes();

            System.arraycopy(paramNameBytes, 0, paramsBuf, paramsIdx, paramNameBytes.length);
            paramsIdx = paramsIdx + paramNameBytes.length;

            paramsBuf[paramsIdx++] = PostgresProtocolGeneralConstants.DELIMITER_BYTE;

            System.arraycopy(paramValueBytes, 0, paramsBuf, paramsIdx, paramValueBytes.length);
            paramsIdx = paramsIdx + paramValueBytes.length;

            paramsBuf[paramsIdx++] = PostgresProtocolGeneralConstants.DELIMITER_BYTE;

            length += paramNameBytes.length;
            length += paramValueBytes.length;
            //delimiter
            length += 2;
        }

        paramsBuf[paramsIdx++] = PostgresProtocolGeneralConstants.DELIMITER_BYTE;

        ByteBuf buf = allocator.buffer(length + 1);

        buf.writeInt(length);
        buf.writeShort(startupMessage.getMajorVersion());
        buf.writeShort(startupMessage.getMinorVersion());
        buf.writeBytes(paramsBuf, 0, paramsIdx);

        return buf;
    }

    public static ByteBuf encodeSaslInitialResponseMessage(SaslInitialResponse saslInitialResponse, ByteBufAllocator allocator) {
        byte[] mechanismNameBytes = saslInitialResponse.getNameOfSaslAuthMechanism().getBytes();
        byte[] saslSpecificData = saslInitialResponse.getSaslMechanismSpecificData().getBytes();

        //length (int32) + 1 delimiter byte + length of sasl data (int32)
        int length = 9 + mechanismNameBytes.length + saslSpecificData.length;

        ByteBuf buf = allocator.buffer(length + 1);

        buf.writeByte(PostgresProtocolGeneralConstants.CLIENT_PASSWORD_RESPONSE_START_CHAR);
        buf.writeInt(length);
        buf.writeBytes(mechanismNameBytes);
        buf.writeByte(PostgresProtocolGeneralConstants.DELIMITER_BYTE);
        buf.writeInt(saslSpecificData.length);
        buf.writeBytes(saslSpecificData);

        return buf;
    }

    public static ByteBuf encodeSaslResponseMessage(SaslResponse saslResponse, ByteBufAllocator allocator) {
        byte[] saslSpecificData = saslResponse.getSaslMechanismSpecificData().getBytes();

        //length (int32)
        int length = 4 + saslSpecificData.length;

        ByteBuf buf = allocator.buffer(length + 1);

        buf.writeByte(PostgresProtocolGeneralConstants.CLIENT_PASSWORD_RESPONSE_START_CHAR);
        buf.writeInt(length);
        buf.writeBytes(saslSpecificData);

        return buf;
    }
}
