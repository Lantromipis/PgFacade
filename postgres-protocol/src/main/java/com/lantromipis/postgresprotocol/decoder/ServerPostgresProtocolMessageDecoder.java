package com.lantromipis.postgresprotocol.decoder;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolErrorAndNoticeConstant;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.exception.MessageDecodingException;
import com.lantromipis.postgresprotocol.model.protocol.*;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.postgresprotocol.utils.TempFastThreadLocalStorageUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ServerPostgresProtocolMessageDecoder {
    public static AuthenticationRequestMessage decodeAuthRequestMessage(ByteBuf byteBuf) {
        try {
            byte marker = byteBuf.readByte();

            if (marker != PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR) {
                throw new MessageDecodingException("Received message with wrong start char.");
            }

            int length = byteBuf.readInt();
            int methodMarker = byteBuf.readInt();

            PostgresProtocolAuthenticationMethod postgresProtocolAuthenticationMethod = null;

            for (PostgresProtocolAuthenticationMethod method : PostgresProtocolAuthenticationMethod.values()) {
                if (method.getProtocolMethodMarker() == methodMarker) {
                    postgresProtocolAuthenticationMethod = method;
                    break;
                }
            }

            // marker + length(int32) + methodMarker(int32)
            int methodSpecificDataLength = length - 9;

            String parametersSpecificDataString = byteBuf.toString(byteBuf.readerIndex(), methodSpecificDataLength, StandardCharsets.UTF_8);

            return AuthenticationRequestMessage
                    .builder()
                    .method(postgresProtocolAuthenticationMethod)
                    .specificData(parametersSpecificDataString)
                    .build();

        } catch (Exception e) {
            throw new MessageDecodingException("Error decoding AuthRequest. ", e);
        } finally {
            byteBuf.resetReaderIndex();
        }
    }

    public static AuthenticationSASLContinue decodeAuthSaslContinueMessage(ByteBuf byteBuf) {
        try {
            byte marker = byteBuf.readByte();

            if (marker != PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR) {
                throw new MessageDecodingException("Received message with wrong start char.");
            }

            //length (int32) + challenge marker (int32)
            int length = byteBuf.readInt() - 8;
            int saslChallengeMarker = byteBuf.readInt();

            if (saslChallengeMarker != PostgresProtocolGeneralConstants.SASL_AUTH_CHALLENGE_MARKER) {
                throw new MessageDecodingException("Not a SASL challenge message.");
            }

            byte[] messageBytes = TempFastThreadLocalStorageUtils.getThreadLocalByteArray();
            byteBuf.readBytes(messageBytes, 0, length);

            return AuthenticationSASLContinue
                    .builder()
                    .saslMechanismSpecificData(new String(messageBytes, 0, length))
                    .build();

        } catch (Exception e) {
            throw new MessageDecodingException("Error decoding AuthenticationSASLContinue. ", e);
        } finally {
            byteBuf.resetReaderIndex();
        }
    }

    public static RowDescription decodeRowDescriptionMessage(ByteBuf byteBuf) {
        byte marker = byteBuf.readByte();

        if (marker != PostgresProtocolGeneralConstants.ROW_DESCRIPTION_START_CHAR) {
            throw new MessageDecodingException("Received message with wrong start char.");
        }

        byteBuf.readerIndex(byteBuf.readerIndex() + 4);

        short numOfFieldsInRow = byteBuf.readShort();
        List<RowDescription.FieldDescription> fieldDescriptions = new ArrayList<>(numOfFieldsInRow);

        for (short i = 0; i < numOfFieldsInRow; i++) {
            String fieldName = DecoderUtils.readNextNullTerminatedString(byteBuf);

            int tableOid = byteBuf.readInt();
            short columnAttributeNumber = byteBuf.readShort();
            int fieldDataTypeOid = byteBuf.readInt();
            short fieldDataTypeSize = byteBuf.readShort();
            int typeModifier = byteBuf.readInt();
            short formatCode = byteBuf.readShort();

            fieldDescriptions.add(
                    RowDescription.FieldDescription
                            .builder()
                            .fieldName(fieldName)
                            .tableOid(tableOid)
                            .columnAttributeNumber(columnAttributeNumber)
                            .fieldDataTypeOid(fieldDataTypeOid)
                            .fieldDataTypeSize(fieldDataTypeSize)
                            .typeModifier(typeModifier)
                            .formatCode(formatCode)
                            .build()
            );
        }

        return RowDescription
                .builder()
                .fieldDescriptions(fieldDescriptions)
                .build();
    }

    public static DataRow decodeDataRowMessage(ByteBuf byteBuf) {
        byte marker = byteBuf.readByte();

        if (marker != PostgresProtocolGeneralConstants.DATA_ROW_START_CHAR) {
            throw new MessageDecodingException("Received message with wrong start char.");
        }

        // read length
        byteBuf.readerIndex(byteBuf.readerIndex() + 4);

        short numberOfColumns = byteBuf.readShort();
        List<byte[]> columns = new ArrayList<>(numberOfColumns);

        for (short i = 0; i < numberOfColumns; i++) {
            int columnLength = byteBuf.readInt();
            if (columnLength == -1) {
                columns.add(null);
            } else {
                byte[] columnData = new byte[columnLength];
                byteBuf.readBytes(columnData, 0, columnLength);
                columns.add(columnData);
            }
        }

        return DataRow
                .builder()
                .columns(columns)
                .build();
    }

    public static CommandComplete decodeCommandCompleteMessage(ByteBuf byteBuf) {
        byte marker = byteBuf.readByte();

        if (marker != PostgresProtocolGeneralConstants.COMMAND_COMPLETE_START_CHAR) {
            throw new MessageDecodingException("Received message with wrong start char.");
        }

        // read length
        int length = byteBuf.readInt() - 4;
        byte[] buf = TempFastThreadLocalStorageUtils.getThreadLocalByteArray();

        // skip null terminator
        byteBuf.readBytes(buf, 0, length - 1);

        return CommandComplete
                .builder()
                .commandTag(new String(buf, 0, buf.length, StandardCharsets.UTF_8))
                .build();
    }

    public static ErrorResponse decodeErrorResponse(ByteBuf byteBuf) {
        byte marker = byteBuf.readByte();

        if (marker != PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR) {
            throw new MessageDecodingException("Received message with wrong start char.");
        }

        ErrorResponse ret = new ErrorResponse();
        // read length
        byteBuf.readerIndex(byteBuf.readerIndex() + 4);

        while (byteBuf.readerIndex() < byteBuf.writerIndex()) {
            byte fieldType = byteBuf.readByte();

            switch (fieldType) {
                case PostgresProtocolGeneralConstants.DELIMITER_BYTE -> {
                    return ret;
                }
                case PostgresProtocolErrorAndNoticeConstant.SQLSTATE_CODE_MARKER ->
                        ret.setCode(DecoderUtils.readNextNullTerminatedString(byteBuf));
                case PostgresProtocolErrorAndNoticeConstant.MESSAGE_MARKER ->
                        ret.setMessage(DecoderUtils.readNextNullTerminatedString(byteBuf));
                default -> {
                    DecoderUtils.readNextNullTerminatedString(byteBuf);
                }
            }
        }

        return ret;
    }
}
