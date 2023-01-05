package com.lantromipis.postgresprotocol.decoder;

import com.lantromipis.postgresprotocol.constant.PostgreSQLProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.exception.MessageDecodingException;
import com.lantromipis.postgresprotocol.model.SaslInitialResponse;
import com.lantromipis.postgresprotocol.model.SaslResponse;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ClientPostgreSqlProtocolMessageDecoder {

    public static StartupMessage decodeStartupMessage(ByteBuf byteBuf) throws MessageDecodingException {
        try {
            int length = byteBuf.readInt();
            short majorVersion = byteBuf.readShort();
            short minorVersion = byteBuf.readShort();

            Map<String, String> parameters = new HashMap<>();

            //int (4 bytes) + short (2 bytes) + short (2 bytes)
            int parametersLength = length - 8;

            byte[] parameterBytes = new byte[parametersLength];
            byteBuf.readBytes(parameterBytes, 0, parametersLength);

            String parametersString = new String(parameterBytes, StandardCharsets.UTF_8);
            String[] paramsNamesWithValues = parametersString.split(PostgreSQLProtocolGeneralConstants.DELIMITER_BYTE_CHAR);

            for (int i = 0; i < paramsNamesWithValues.length; i += 2) {
                parameters.put(paramsNamesWithValues[i], paramsNamesWithValues[i + 1]);
            }
            return StartupMessage
                    .builder()
                    .majorVersion(majorVersion)
                    .minorVersion(minorVersion)
                    .parameters(parameters)
                    .build();
        } catch (Exception e) {
            throw new MessageDecodingException("Error decoding StartupMessage. ", e);
        } finally {
            byteBuf.resetReaderIndex();
        }
    }

    public static SaslInitialResponse decodeSaslInitialResponse(ByteBuf byteBuf) throws MessageDecodingException {
        try {
            SaslInitialResponse saslInitialResponse = new SaslInitialResponse();

            byte firstByte = byteBuf.readByte();

            if (firstByte != PostgreSQLProtocolGeneralConstants.CLIENT_PASSWORD_RESPONSE_START_CHAR) {
                throw new MessageDecodingException("Received message with wrong start char.");
            }

            int totalLength = byteBuf.readInt();

            //4 bytes for length (int32)
            int messageLength = totalLength - 4;

            byte[] messageBytes = new byte[messageLength];
            byteBuf.readBytes(messageBytes, 0, messageLength);

            int saslMechanismNameEndByteIndex = 0;
            for (int i = 0; i < messageBytes.length; i++) {
                if (messageBytes[i] == PostgreSQLProtocolGeneralConstants.DELIMITER_BYTE) {
                    saslMechanismNameEndByteIndex = i;
                    break;
                }
            }
            saslInitialResponse.setNameOfSaslAuthMechanism(
                    new String(
                            messageBytes,
                            0,
                            saslMechanismNameEndByteIndex,
                            StandardCharsets.UTF_8
                    )
            );

            //4 bytes for specific data length (int32)
            int saslMechanismSpecificDataFirstByteIndex = saslMechanismNameEndByteIndex + 5;
            int saslMechanismSpecificDataLength = messageLength - saslMechanismSpecificDataFirstByteIndex;
            if (saslMechanismSpecificDataLength > 0) {
                saslInitialResponse.setSaslMechanismSpecificData(
                        new String(
                                messageBytes,
                                saslMechanismSpecificDataFirstByteIndex,
                                saslMechanismSpecificDataLength,
                                StandardCharsets.UTF_8
                        )
                );
            }

            return saslInitialResponse;
        } catch (Exception e) {
            if (e instanceof MessageDecodingException) {
                throw e;
            } else {
                throw new MessageDecodingException("Error decoding SASLInitialResponse. ", e);
            }
        } finally {
            byteBuf.resetReaderIndex();
        }
    }

    public static SaslResponse decodeSaslResponse(ByteBuf byteBuf) throws MessageDecodingException {
        try {
            byte firstByte = byteBuf.readByte();

            if (firstByte != PostgreSQLProtocolGeneralConstants.CLIENT_PASSWORD_RESPONSE_START_CHAR) {
                throw new MessageDecodingException("Received message with wrong start char.");
            }

            //4 bytes for length (int32)
            int length = byteBuf.readInt() - 4;

            byte[] messageBytes = new byte[length];
            byteBuf.readBytes(messageBytes, 0, length);

            return new SaslResponse(new String(messageBytes, StandardCharsets.UTF_8));
        } catch (Exception e) {
            if (e instanceof MessageDecodingException) {
                throw e;
            } else {
                throw new MessageDecodingException("Error decoding SASLInitialResponse. ", e);
            }
        } finally {
            byteBuf.resetReaderIndex();
        }
    }
}
