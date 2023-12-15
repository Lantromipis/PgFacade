package com.lantromipis.postgresprotocol.decoder;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.exception.MessageDecodingException;
import com.lantromipis.postgresprotocol.model.protocol.AuthenticationRequestMessage;
import com.lantromipis.postgresprotocol.model.protocol.AuthenticationSASLContinue;
import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import com.lantromipis.postgresprotocol.utils.TempFastThreadLocalStorageUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class ServerPostgresProtocolMessageDecoder {
    public static AuthenticationRequestMessage decodeAuthRequestMessage(ByteBuf byteBuf) {
        try {
            byte marker = byteBuf.readByte();
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
}
