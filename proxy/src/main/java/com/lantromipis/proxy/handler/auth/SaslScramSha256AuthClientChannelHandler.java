package com.lantromipis.proxy.handler.auth;

import com.lantromipis.connectionpool.model.ScramAuthInfo;
import com.lantromipis.postgresprotocol.constant.PostgreSQLProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.constant.PostgreSQLProtocolScramConstants;
import com.lantromipis.postgresprotocol.decoder.ClientPostgreSqlProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ServerPostgreSqlProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.SaslInitialResponse;
import com.lantromipis.postgresprotocol.model.SaslResponse;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.postgresprotocol.utils.ProtocolUtils;
import com.lantromipis.postgresprotocol.utils.ScramUtils;
import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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
    private final ProtocolUtils protocolUtils;

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

    private SaslAuthStatus saslAuthStatus = SaslAuthStatus.NOT_STARTED;
    private final StartupMessage startupMessage;

    public SaslScramSha256AuthClientChannelHandler(StartupMessage startupMessage, UserAuthInfoProvider userAuthInfoProvider, ProxyChannelHandlersProducer proxyChannelHandlersProducer, ProtocolUtils protocolUtils) {
        this.startupMessage = startupMessage;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
        this.protocolUtils = protocolUtils;

        String username = startupMessage.getParameters().get(PostgreSQLProtocolGeneralConstants.STARTUP_PARAMETER_USER);
        String passwd = userAuthInfoProvider.getPasswdForUser(username);

        Pattern passwdPattern = PostgreSQLProtocolScramConstants.SCRAM_SHA_256_PASSWD_FORMAT_PATTERN;
        Matcher passwdMatcher = passwdPattern.matcher(passwd);
        passwdMatcher.matches();

        this.iterationCount = Integer.parseInt(passwdMatcher.group(1));
        this.salt = passwdMatcher.group(2);
        this.storedKey = passwdMatcher.group(3);
        this.serverKey = passwdMatcher.group(4);

        this.serverNonce = UUID.randomUUID().toString();
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
    }

    private void processFirstMessage(ChannelHandlerContext ctx, Object msg) {
        SaslInitialResponse saslInitialResponse = ClientPostgreSqlProtocolMessageDecoder.decodeSaslInitialResponse((ByteBuf) msg);

        if (!saslInitialResponse.getNameOfSaslAuthMechanism().equals(PostgreSQLProtocolScramConstants.SASL_SHA_256_AUTH_MECHANISM_NAME)) {
            log.error("SCRAM-SAH-256 was not chosen by client as SASL mechanism.");
            rejectRequest(ctx);
            return;
        }

        Pattern firstClientMessagePattern = PostgreSQLProtocolScramConstants.CLIENT_FIRST_MESSAGE_PATTERN;
        Matcher firstClientMessageMatcher = firstClientMessagePattern.matcher(saslInitialResponse.getSaslMechanismSpecificData());
        if (!firstClientMessageMatcher.matches()) {
            log.error("Error reading SASLInitialResponse mechanism specific data.");
            rejectRequest(ctx);
            return;
        }

        gs2Header = firstClientMessageMatcher.group(PostgreSQLProtocolScramConstants.CLIENT_FIRST_MESSAGE_GS2_HEADER_MATCHER_GROUP);
        clientFirstMessageBare = firstClientMessageMatcher.group(PostgreSQLProtocolScramConstants.CLIENT_FIRST_MESSAGE_BARE_MATCHER_GROUP);
        clientNonce = firstClientMessageMatcher.group(PostgreSQLProtocolScramConstants.CLIENT_FIRST_MESSAGE_NONCE_MATCHER_GROUP);

        String combinedNonce = clientNonce + serverNonce;
        serverFirstMessage = String.format(PostgreSQLProtocolScramConstants.SERVER_FIRST_MESSAGE_FORMAT, combinedNonce, salt, iterationCount);

        ByteBuf responseBuf = ServerPostgreSqlProtocolMessageEncoder.createAuthenticationSaslContinueMessage(serverFirstMessage);

        saslAuthStatus = SaslAuthStatus.FIRST_CLIENT_MESSAGE_RECEIVED;
        ctx.channel().writeAndFlush(responseBuf);
        ctx.channel().read();
    }

    private void processFinalMessage(ChannelHandlerContext ctx, Object msg) {
        SaslResponse saslResponse = ClientPostgreSqlProtocolMessageDecoder.decodeSaslResponse((ByteBuf) msg);

        Pattern saslFinalMessagePattern = PostgreSQLProtocolScramConstants.CLIENT_FINAL_MESSAGE_PATTERN;
        Matcher saslFinalMessageMatcher = saslFinalMessagePattern.matcher(saslResponse.getSaslMechanismSpecificData());

        if (!saslFinalMessageMatcher.matches()) {
            log.error("Error reading SASLResponse mechanism specific data.");
            rejectRequest(ctx);
            return;
        }

        String newGs2Header = new String(Base64.getDecoder().decode(saslFinalMessageMatcher.group(PostgreSQLProtocolScramConstants.CLIENT_FINAL_MESSAGE_GS2_HEADER_MATCHER_GROUP)));

        if (!newGs2Header.equals(gs2Header)) {
            log.error("GS2 header from client is not equal to stored one.");
            rejectRequest(ctx);
            return;
        }

        String finalClientNonce = saslFinalMessageMatcher.group(PostgreSQLProtocolScramConstants.CLIENT_FINAL_MESSAGE_NONCE_MATCHER_GROUP);

        if (!finalClientNonce.equals(clientNonce + serverNonce)) {
            log.error("SASL nonce received from client is not equal to stored one.");
            rejectRequest(ctx);
            return;
        }

        String clientProof = saslFinalMessageMatcher.group(PostgreSQLProtocolScramConstants.CLIENT_FINAL_MESSAGE_PROOF_MATCHER_GROUP);
        String finalMessageWithoutProof = saslFinalMessageMatcher.group(PostgreSQLProtocolScramConstants.CLIENT_FINAL_MESSAGE_WITHOUT_PROOF_MATCHER_GROUP);

        String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + finalMessageWithoutProof;

        byte[] storedKeyBytes = Base64.getDecoder().decode(storedKey);
        byte[] serverKeyBytes = Base64.getDecoder().decode(serverKey);

        try {
            byte[] clientSignature = ScramUtils.computeHmac(storedKeyBytes, PostgreSQLProtocolScramConstants.SHA256_HMAC_NAME, authMessage);
            byte[] serverSignature = ScramUtils.computeHmac(serverKeyBytes, PostgreSQLProtocolScramConstants.SHA256_HMAC_NAME, authMessage);
            byte[] proofBytes = Base64.getDecoder().decode(clientProof);

            byte[] computedClientKey = clientSignature.clone();
            for (int i = 0; i < computedClientKey.length; i++) {
                computedClientKey[i] ^= proofBytes[i];
            }

            byte[] hashedComputedKey = MessageDigest.getInstance(PostgreSQLProtocolScramConstants.SHA256_DIGEST_NAME).digest(computedClientKey);
            if (!Arrays.equals(storedKeyBytes, hashedComputedKey)) {
                log.error("Incorrect password provided");
                rejectRequest(ctx);
                return;
            }

            String saslServerFinalMessage = "v=" + new String(Base64.getEncoder().encode(serverSignature), StandardCharsets.UTF_8);

            ByteBuf finalSaslResponse = ServerPostgreSqlProtocolMessageEncoder.createAuthenticationSASLFinalMessage(saslServerFinalMessage);
            ByteBuf authOkResponse = ServerPostgreSqlProtocolMessageEncoder.createAuthenticationOkMessage();
            ByteBuf serverParametersStatusResponse = protocolUtils.getServerParametersStatusMessage();
            ByteBuf readyForQueryResponse = ServerPostgreSqlProtocolMessageEncoder.encodeReadyForQueryMessage();

            ByteBuf combinedMessage = Unpooled.copiedBuffer(finalSaslResponse, authOkResponse, serverParametersStatusResponse, readyForQueryResponse);

            ctx.channel().pipeline().addLast(
                    proxyChannelHandlersProducer.createNewSessionPooledConnectionHandler(
                            startupMessage,
                            ScramAuthInfo
                                    .builder()
                                    .clientKey(computedClientKey)
                                    .storedKeyBase64(storedKey)
                                    .build()
                    )
            );
            ctx.channel().pipeline().remove(this);

            ctx.channel().writeAndFlush(combinedMessage);
            ctx.channel().read();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rejectRequest(ctx);
        }
    }
}
