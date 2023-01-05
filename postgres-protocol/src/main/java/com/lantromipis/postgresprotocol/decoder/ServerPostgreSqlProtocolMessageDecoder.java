package com.lantromipis.postgresprotocol.decoder;

import com.lantromipis.postgresprotocol.constant.PostgreSQLProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.exception.MessageDecodingException;
import com.lantromipis.postgresprotocol.model.AuthenticationMethod;
import com.lantromipis.postgresprotocol.model.AuthenticationRequestMessage;
import com.lantromipis.postgresprotocol.model.AuthenticationSASLContinue;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class ServerPostgreSqlProtocolMessageDecoder {
    public static AuthenticationRequestMessage decodeAuthRequestMessage(ByteBuf byteBuf) {
        try {
            byte marker = byteBuf.readByte();
            int length = byteBuf.readInt();
            int methodMarker = byteBuf.readInt();

            AuthenticationMethod authenticationMethod = null;

            for (AuthenticationMethod method : AuthenticationMethod.values()) {
                if (method.getProtocolMethodMarker() == methodMarker) {
                    authenticationMethod = method;
                    break;
                }
            }

            //marker + length(int32) + methodMarker(int32)
            int methodSpecificDataLength = length - 9;

            byte[] methodSpecificDataBytes = new byte[methodSpecificDataLength];
            byteBuf.readBytes(methodSpecificDataBytes, 0, methodSpecificDataLength);

            String parametersSpecificDataString = new String(methodSpecificDataBytes, StandardCharsets.UTF_8);

            return AuthenticationRequestMessage
                    .builder()
                    .method(authenticationMethod)
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

            if (marker != PostgreSQLProtocolGeneralConstants.AUTH_REQUEST_START_CHAR) {
                throw new MessageDecodingException("Received message with wrong start char.");
            }

            //length (int32) + challenge marker (int32)
            int length = byteBuf.readInt() - 8;
            int saslChallengeMarker = byteBuf.readInt();

            if (saslChallengeMarker != PostgreSQLProtocolGeneralConstants.SASL_AUTH_CHALLENGE_MARKER) {
                throw new MessageDecodingException("Not a SASL challenge message.");
            }

            byte[] messageBytes = new byte[length];
            byteBuf.readBytes(messageBytes, 0, length);

            return AuthenticationSASLContinue
                    .builder()
                    .saslMechanismSpecificData(new String(messageBytes))
                    .build();

        } catch (Exception e) {
            throw new MessageDecodingException("Error decoding AuthenticationSASLContinue. ", e);
        } finally {
            byteBuf.resetReaderIndex();
        }
    }
}
