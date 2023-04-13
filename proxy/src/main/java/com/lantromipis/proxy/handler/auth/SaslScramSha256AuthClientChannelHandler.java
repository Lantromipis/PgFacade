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
import io.netty.buffer.Unpooled;
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

    //server props
    private final int iterationCount;
    private final String salt;
    private final String storedKey;
    private final String serverKey;
    private final String serverNonce;
    private String serverFirstMessage;

    //client props
    private String gs2Header;
    private String clientFirstMessageBare;
    private String clientNonce;

    private String username;
    private SaslAuthStatus saslAuthStatus;
    private final StartupMessage startupMessage;

    public SaslScramSha256AuthClientChannelHandler(StartupMessage startupMessage, UserAuthInfoProvider userAuthInfoProvider, ProxyChannelHandlersProducer proxyChannelHandlersProducer) {
        this.startupMessage = startupMessage;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
        this.saslAuthStatus = SaslAuthStatus.NOT_STARTED;

        username = startupMessage.getParameters().get(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER);
        String passwd = userAuthInfoProvider.getPasswdForUser(username);

        Pattern passwdPattern = PostgresProtocolScramConstants.SCRAM_SHA_256_PASSWD_FORMAT_PATTERN;
        Matcher passwdMatcher = passwdPattern.matcher(passwd);
        passwdMatcher.matches();

        this.iterationCount = Integer.parseInt(passwdMatcher.group(1));
        this.salt = passwdMatcher.group(2);
        this.storedKey = passwdMatcher.group(3);
        this.serverKey = passwdMatcher.group(4);

        this.serverNonce = UUID.randomUUID().toString();
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

        gs2Header = firstClientMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FIRST_MESSAGE_GS2_HEADER_MATCHER_GROUP);
        clientFirstMessageBare = firstClientMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FIRST_MESSAGE_BARE_MATCHER_GROUP);
        clientNonce = firstClientMessageMatcher.group(PostgresProtocolScramConstants.CLIENT_FIRST_MESSAGE_NONCE_MATCHER_GROUP);

        String combinedNonce = clientNonce + serverNonce;
        serverFirstMessage = String.format(PostgresProtocolScramConstants.SERVER_FIRST_MESSAGE_FORMAT, combinedNonce, salt, iterationCount);

        ByteBuf responseBuf = ServerPostgresProtocolMessageEncoder.createAuthenticationSaslContinueMessage(serverFirstMessage, ctx.alloc());

        saslAuthStatus = SaslAuthStatus.FIRST_CLIENT_MESSAGE_RECEIVED;
        ctx.channel().writeAndFlush(responseBuf);
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

        byte[] storedKeyBytes = Base64.getDecoder().decode(storedKey);
        byte[] serverKeyBytes = Base64.getDecoder().decode(serverKey);

        byte[] clientSignature = ScramUtils.computeHmac(storedKeyBytes, PostgresProtocolScramConstants.SHA256_HMAC_NAME, authMessage);
        byte[] serverSignature = ScramUtils.computeHmac(serverKeyBytes, PostgresProtocolScramConstants.SHA256_HMAC_NAME, authMessage);
        byte[] proofBytes = Base64.getDecoder().decode(clientProof);

        byte[] computedClientKey = clientSignature.clone();
        for (int i = 0; i < computedClientKey.length; i++) {
            computedClientKey[i] ^= proofBytes[i];
        }

        byte[] hashedComputedKey = MessageDigest.getInstance(PostgresProtocolScramConstants.SHA256_DIGEST_NAME).digest(computedClientKey);
        if (!Arrays.equals(storedKeyBytes, hashedComputedKey)) {
            log.debug("Incorrect password provided");
            forceCloseConnectionWithAuthError();
            return;
        }

        String saslServerFinalMessage = "v=" + new String(Base64.getEncoder().encode(serverSignature), StandardCharsets.UTF_8);

        ByteBuf finalSaslResponse = ServerPostgresProtocolMessageEncoder.createAuthenticationSASLFinalMessage(saslServerFinalMessage, ctx.alloc());
        ByteBuf authOkResponse = ServerPostgresProtocolMessageEncoder.createAuthenticationOkMessage(ctx.alloc());
        ByteBuf combinedMessage = Unpooled.copiedBuffer(finalSaslResponse, authOkResponse);

        proxyChannelHandlersProducer.getNewSessionPooledConnectionHandlerByCallback(
                startupMessage,
                ScramAuthInfo
                        .builder()
                        .clientKey(computedClientKey)
                        .storedKeyBase64(storedKey)
                        .build(),
                combinedMessage,
                handler -> {
                    ctx.channel().pipeline().addLast(handler);
                    ctx.channel().pipeline().remove(this);
                    setActive(false);
                }
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
