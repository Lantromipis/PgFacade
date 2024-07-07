package com.lantromipis.proxy.auth;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolScramConstants;
import com.lantromipis.postgresprotocol.decoder.ClientPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.auth.ScramPgAuthInfo;
import com.lantromipis.postgresprotocol.model.protocol.SaslInitialResponse;
import com.lantromipis.postgresprotocol.model.protocol.SaslResponse;
import com.lantromipis.postgresprotocol.utils.PostgresErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.PostgresHandlerUtils;
import com.lantromipis.postgresprotocol.utils.ScramUtils;
import com.lantromipis.usermanagement.model.ScramSha256UserAuthInfo;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.Mac;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class ScramSha256AuthProcessor implements ProxyAuthProcessor {
    private enum SaslAuthStatus {
        NOT_STARTED,
        FIRST_CLIENT_MESSAGE_RECEIVED,
    }

    //server props
    private final int iterationCount;
    private final String salt;
    private final byte[] storedKeyDecodedBytes;
    private final byte[] serverKeyDecodedBytes;
    private final String serverNonce;
    private String serverFirstMessage;
    private String authMessageWithoutFinalMessage;
    // one-times Mac. Initialized while waiting for client response for performance reasons.
    private Mac storedKeyMac;
    private Mac serverKeyMac;

    //client props
    private String gs2Header;
    private String clientFirstMessageBare;
    private String clientNonce;

    private final String username;
    private SaslAuthStatus saslAuthStatus;

    public ScramSha256AuthProcessor(ScramSha256UserAuthInfo scramSha256UserAuthInfo) {
        this.username = scramSha256UserAuthInfo.getUsername();
        this.saslAuthStatus = SaslAuthStatus.NOT_STARTED;

        this.iterationCount = scramSha256UserAuthInfo.getIterationCount();
        this.salt = scramSha256UserAuthInfo.getSalt();

        this.storedKeyDecodedBytes = scramSha256UserAuthInfo.getStoredKeyDecodedBytes();
        this.serverKeyDecodedBytes = scramSha256UserAuthInfo.getServerKeyDecodedBytes();

        this.serverNonce = UUID.randomUUID().toString();
    }

    @Override
    public ScramPgAuthInfo processAuth(ChannelHandlerContext ctx, ByteBuf message) throws Exception {
        switch (saslAuthStatus) {
            case NOT_STARTED -> {
                processFirstMessage(ctx, message);
            }
            case FIRST_CLIENT_MESSAGE_RECEIVED -> {
                return processFinalMessage(ctx, message);
            }
        }

        return null;
    }

    private void processFirstMessage(ChannelHandlerContext ctx, ByteBuf msg) throws NoSuchAlgorithmException, InvalidKeyException {
        SaslInitialResponse saslInitialResponse = ClientPostgresProtocolMessageDecoder.decodeSaslInitialResponse(msg);

        if (!saslInitialResponse.getNameOfSaslAuthMechanism().equals(PostgresProtocolScramConstants.SASL_SHA_256_AUTH_MECHANISM_NAME)) {
            log.error("SCRAM-SAH-256 was not chosen by client as SASL mechanism, but was expected to.");
            forceCloseConnectionWithAuthError(ctx.channel());
            return;
        }

        Pattern firstClientMessagePattern = PostgresProtocolScramConstants.CLIENT_FIRST_MESSAGE_PATTERN;
        Matcher firstClientMessageMatcher = firstClientMessagePattern.matcher(saslInitialResponse.getSaslMechanismSpecificData());
        if (!firstClientMessageMatcher.matches()) {
            log.error("Error reading SASLInitialResponse mechanism specific data.");
            forceCloseConnectionWithAuthError(ctx.channel());
            return;
        }

        clientNonce = firstClientMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FIRST_MESSAGE_NONCE_MATCHER_GROUP);

        serverFirstMessage =
                "r=" + clientNonce + serverNonce +
                        ",s=" + salt +
                        ",i=" + iterationCount;

        ctx.channel().writeAndFlush(ServerPostgresProtocolMessageEncoder.createAuthenticationSaslContinueMessage(serverFirstMessage, ctx.alloc()));

        // free actions for performance, because response was sent
        gs2Header = firstClientMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FIRST_MESSAGE_GS2_HEADER_MATCHER_GROUP);
        clientFirstMessageBare = firstClientMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FIRST_MESSAGE_BARE_MATCHER_GROUP);

        authMessageWithoutFinalMessage = clientFirstMessageBare + "," + serverFirstMessage + ",";

        storedKeyMac = ScramUtils.createHmac(storedKeyDecodedBytes, PostgresProtocolScramConstants.SHA256_HMAC_NAME);
        serverKeyMac = ScramUtils.createHmac(serverKeyDecodedBytes, PostgresProtocolScramConstants.SHA256_HMAC_NAME);

        saslAuthStatus = SaslAuthStatus.FIRST_CLIENT_MESSAGE_RECEIVED;
    }

    private ScramPgAuthInfo processFinalMessage(ChannelHandlerContext ctx, ByteBuf msg) throws NoSuchAlgorithmException, InvalidKeyException {
        SaslResponse saslResponse = ClientPostgresProtocolMessageDecoder.decodeSaslResponse(msg);

        Pattern saslFinalMessagePattern = PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_PATTERN;
        Matcher saslFinalMessageMatcher = saslFinalMessagePattern.matcher(saslResponse.getSaslMechanismSpecificData());

        if (!saslFinalMessageMatcher.matches()) {
            log.error("Error reading SASLResponse mechanism specific data.");
            forceCloseConnectionWithAuthError(ctx.channel());
            return null;
        }

        String newGs2Header = new String(Base64.getDecoder().decode(saslFinalMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_GS2_HEADER_MATCHER_GROUP)));

        if (!newGs2Header.equals(gs2Header)) {
            log.error("GS2 header from client is not equal to stored one.");
            forceCloseConnectionWithAuthError(ctx.channel());
            return null;
        }

        String finalClientNonce = saslFinalMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_NONCE_MATCHER_GROUP);

        if (!finalClientNonce.equals(clientNonce + serverNonce)) {
            log.error("SASL nonce received from client is not equal to stored one.");
            forceCloseConnectionWithAuthError(ctx.channel());
            return null;
        }

        String clientProof = saslFinalMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_PROOF_MATCHER_GROUP);
        String finalMessageWithoutProof = saslFinalMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_WITHOUT_PROOF_MATCHER_GROUP);

        String authMessage = authMessageWithoutFinalMessage + finalMessageWithoutProof;
        byte[] authMessageBytes = authMessage.getBytes(StandardCharsets.US_ASCII);

        byte[] clientSignature = ScramUtils.computeHmac(storedKeyMac, authMessageBytes);
        byte[] serverSignature = ScramUtils.computeHmac(serverKeyMac, authMessageBytes);
        byte[] proofBytes = Base64.getDecoder().decode(clientProof);

        byte[] computedClientKey = clientSignature.clone();
        for (int i = 0; i < computedClientKey.length; i++) {
            computedClientKey[i] ^= proofBytes[i];
        }

        byte[] hashedComputedKey = MessageDigest.getInstance(PostgresProtocolScramConstants.SHA256_DIGEST_NAME).digest(computedClientKey);
        if (!Arrays.equals(storedKeyDecodedBytes, hashedComputedKey)) {
            log.debug("Incorrect password provided");
            forceCloseConnectionWithAuthError(ctx.channel());
            return null;
        }

        String saslServerFinalMessage = "v=" + new String(Base64.getEncoder().encode(serverSignature), StandardCharsets.UTF_8);

        ctx.writeAndFlush(ServerPostgresProtocolMessageEncoder.createAuthenticationSASLFinalMessageWithAuthOk(saslServerFinalMessage, ctx.alloc()));

        return ScramPgAuthInfo
                .builder()
                .passwordKnown(false)
                .clientKey(computedClientKey)
                .storedKey(storedKeyDecodedBytes)
                .build();
    }

    private void forceCloseConnectionWithAuthError(Channel channel) {
        PostgresHandlerUtils.closeOnFlush(channel, PostgresErrorMessageUtils.getAuthFailedForUserErrorMessage(username, channel.alloc()));
    }
}
