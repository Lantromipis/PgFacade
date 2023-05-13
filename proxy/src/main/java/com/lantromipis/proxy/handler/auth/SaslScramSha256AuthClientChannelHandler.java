package com.lantromipis.proxy.handler.auth;

import com.lantromipis.connectionpool.model.auth.ScramAuthInfo;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolScramConstants;
import com.lantromipis.postgresprotocol.decoder.ClientPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.protocol.SaslInitialResponse;
import com.lantromipis.postgresprotocol.model.protocol.SaslResponse;
import com.lantromipis.postgresprotocol.model.protocol.StartupMessage;
import com.lantromipis.postgresprotocol.utils.ErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.postgresprotocol.utils.ScramUtils;
import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

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
public class SaslScramSha256AuthClientChannelHandler extends AbstractClientChannelHandler {
    private enum SaslAuthStatus {
        NOT_STARTED,
        FIRST_CLIENT_MESSAGE_RECEIVED,
    }

    //dependencies
    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;
    private final UserAuthInfoProvider userAuthInfoProvider;

    //server props
    private final int iterationCount;
    private final String salt;
    private final String storedKey;
    private byte[] storedKeyDecodedBytes;
    private final String serverKey;
    private byte[] serverKeyDecodedBytes;
    private final String serverNonce;
    private String serverFirstMessage;

    //client props
    private String gs2Header;
    private String clientFirstMessageBare;
    private String clientNonce;

    private String username;
    private SaslAuthStatus saslAuthStatus;
    private final StartupMessage startupMessage;
    private final boolean primaryMode;

    public SaslScramSha256AuthClientChannelHandler(final StartupMessage startupMessage,
                                                   final UserAuthInfoProvider userAuthInfoProvider,
                                                   final ProxyChannelHandlersProducer proxyChannelHandlersProducer,
                                                   final boolean primaryMode) {
        this.startupMessage = startupMessage;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
        this.userAuthInfoProvider = userAuthInfoProvider;
        this.saslAuthStatus = SaslAuthStatus.NOT_STARTED;

        username = startupMessage.getParameters().get(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER);
        String passwd = userAuthInfoProvider.getPasswdForUser(username);

        Matcher passwdMatcher = PostgresProtocolScramConstants.SCRAM_SHA_256_PASSWD_FORMAT_PATTERN.matcher(passwd);
        passwdMatcher.matches();

        this.iterationCount = Integer.parseInt(passwdMatcher.group(1));
        this.salt = passwdMatcher.group(2);
        this.storedKey = passwdMatcher.group(3);
        this.serverKey = passwdMatcher.group(4);

        this.serverNonce = UUID.randomUUID().toString();
        this.primaryMode = primaryMode;
    }

    @Override
    public void forceDisconnectAndClearResources() {
        HandlerUtils.closeOnFlush(getInitialChannelHandlerContext().channel(), ServerPostgresProtocolMessageEncoder.createEmptyErrorMessage());
        setActive(false);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        switch (saslAuthStatus) {
            case NOT_STARTED -> {
                processFirstMessage(ctx, msg);
            }
            case FIRST_CLIENT_MESSAGE_RECEIVED -> {
                processFinalMessage(ctx, msg);
            }
        }
        super.channelRead(ctx, msg);
    }

    private void processFirstMessage(ChannelHandlerContext ctx, Object msg) {
        SaslInitialResponse saslInitialResponse = ClientPostgresProtocolMessageDecoder.decodeSaslInitialResponse((ByteBuf) msg);

        if (!saslInitialResponse.getNameOfSaslAuthMechanism().equals(PostgresProtocolScramConstants.SASL_SHA_256_AUTH_MECHANISM_NAME)) {
            log.error("SCRAM-SAH-256 was not chosen by client as SASL mechanism, but was expected to.");
            forceCloseConnectionWithAuthError();
            return;
        }

        Pattern firstClientMessagePattern = PostgresProtocolScramConstants.CLIENT_FIRST_MESSAGE_PATTERN;
        Matcher firstClientMessageMatcher = firstClientMessagePattern.matcher(saslInitialResponse.getSaslMechanismSpecificData());
        if (!firstClientMessageMatcher.matches()) {
            log.error("Error reading SASLInitialResponse mechanism specific data.");
            forceCloseConnectionWithAuthError();
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

        storedKeyDecodedBytes = Base64.getDecoder().decode(storedKey);
        serverKeyDecodedBytes = Base64.getDecoder().decode(serverKey);

        saslAuthStatus = SaslAuthStatus.FIRST_CLIENT_MESSAGE_RECEIVED;

        ctx.channel().read();
    }

    private void processFinalMessage(ChannelHandlerContext ctx, Object msg) throws NoSuchAlgorithmException, InvalidKeyException {
        SaslResponse saslResponse = ClientPostgresProtocolMessageDecoder.decodeSaslResponse((ByteBuf) msg);

        Pattern saslFinalMessagePattern = PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_PATTERN;
        Matcher saslFinalMessageMatcher = saslFinalMessagePattern.matcher(saslResponse.getSaslMechanismSpecificData());

        if (!saslFinalMessageMatcher.matches()) {
            log.error("Error reading SASLResponse mechanism specific data.");
            forceCloseConnectionWithAuthError();
            return;
        }

        String newGs2Header = new String(Base64.getDecoder().decode(saslFinalMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_GS2_HEADER_MATCHER_GROUP)));

        if (!newGs2Header.equals(gs2Header)) {
            log.error("GS2 header from client is not equal to stored one.");
            forceCloseConnectionWithAuthError();
            return;
        }

        String finalClientNonce = saslFinalMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_NONCE_MATCHER_GROUP);

        if (!finalClientNonce.equals(clientNonce + serverNonce)) {
            log.error("SASL nonce received from client is not equal to stored one.");
            forceCloseConnectionWithAuthError();
            return;
        }

        String clientProof = saslFinalMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_PROOF_MATCHER_GROUP);
        String finalMessageWithoutProof = saslFinalMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_WITHOUT_PROOF_MATCHER_GROUP);

        String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + finalMessageWithoutProof;
        byte[] authMessageBytes = authMessage.getBytes(StandardCharsets.US_ASCII);

        byte[] clientSignature = ScramUtils.computeHmac(storedKeyDecodedBytes, PostgresProtocolScramConstants.SHA256_HMAC_NAME, authMessageBytes);
        byte[] serverSignature = ScramUtils.computeHmac(serverKeyDecodedBytes, PostgresProtocolScramConstants.SHA256_HMAC_NAME, authMessageBytes);
        byte[] proofBytes = Base64.getDecoder().decode(clientProof);

        byte[] computedClientKey = clientSignature.clone();
        for (int i = 0; i < computedClientKey.length; i++) {
            computedClientKey[i] ^= proofBytes[i];
        }

        byte[] hashedComputedKey = MessageDigest.getInstance(PostgresProtocolScramConstants.SHA256_DIGEST_NAME).digest(computedClientKey);
        if (!Arrays.equals(storedKeyDecodedBytes, hashedComputedKey)) {
            log.debug("Incorrect password provided");
            forceCloseConnectionWithAuthError();
            return;
        }

        String saslServerFinalMessage = "v=" + new String(Base64.getEncoder().encode(serverSignature), StandardCharsets.UTF_8);

        ByteBuf finalSaslResponse = ServerPostgresProtocolMessageEncoder.createAuthenticationSASLFinalMessageWithAuthOk(saslServerFinalMessage, ctx.alloc());

        proxyChannelHandlersProducer.getNewSessionPooledConnectionHandlerByCallback(
                startupMessage,
                ScramAuthInfo
                        .builder()
                        .clientKey(computedClientKey)
                        .storedKeyBase64(storedKey)
                        .build(),
                finalSaslResponse,
                handler -> {
                    ctx.channel().pipeline().addLast(handler);
                    ctx.channel().pipeline().remove(this);
                    setActive(false);
                },
                primaryMode
        );
    }

    private void forceCloseConnectionWithAuthError() {
        HandlerUtils.closeOnFlush(getInitialChannelHandlerContext().channel(), ErrorMessageUtils.getAuthFailedForUserErrorMessage(username));
        setActive(false);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception during SASL-SCRUM auth!", cause);
        forceCloseConnectionWithAuthError();
    }
}
