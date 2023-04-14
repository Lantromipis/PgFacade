package com.lantromipis.connectionpool.handler.auth;

import com.lantromipis.connectionpool.handler.ConnectionPoolChannelHandlerProducer;
import com.lantromipis.connectionpool.handler.common.AbstractConnectionPoolClientHandler;
import com.lantromipis.connectionpool.model.PgChannelAuthResult;
import com.lantromipis.connectionpool.model.StartupMessageInfo;
import com.lantromipis.connectionpool.model.auth.ScramAuthInfo;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolScramConstants;
import com.lantromipis.postgresprotocol.decoder.ServerPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.protocol.AuthenticationSASLContinue;
import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import com.lantromipis.postgresprotocol.model.protocol.SaslInitialResponse;
import com.lantromipis.postgresprotocol.model.protocol.SaslResponse;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.postgresprotocol.utils.ScramUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Base64;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class PgChannelSaslScramSha256AuthHandler extends AbstractConnectionPoolClientHandler {

    private enum SaslAuthStatus {
        NOT_STARTED,
        FIRST_CLIENT_MESSAGE_SENT,
        LAST_CLIENT_MESSAGE_SENT
    }

    private SaslAuthStatus saslAuthStatus = SaslAuthStatus.NOT_STARTED;

    private String clientNonce;
    private String clientFirstMessageBare;

    private ConnectionPoolChannelHandlerProducer connectionPoolChannelHandlerProducer;
    private ScramAuthInfo scramAuthInfo;
    private StartupMessageInfo startupMessageInfo;
    private Consumer<PgChannelAuthResult> callbackFunction;

    private SaslAuthStatus authStatus = SaslAuthStatus.NOT_STARTED;

    public PgChannelSaslScramSha256AuthHandler(final ConnectionPoolChannelHandlerProducer connectionPoolChannelHandlerProducer,
                                               final ScramAuthInfo scramAuthInfo,
                                               final StartupMessageInfo startupMessageInfo,
                                               final Consumer<PgChannelAuthResult> callbackFunction) {
        this.clientNonce = UUID.randomUUID().toString();

        this.connectionPoolChannelHandlerProducer = connectionPoolChannelHandlerProducer;
        this.scramAuthInfo = scramAuthInfo;
        this.startupMessageInfo = startupMessageInfo;
        this.callbackFunction = callbackFunction;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        clientFirstMessageBare = String.format(PostgresProtocolScramConstants.CLIENT_FIRST_MESSAGE_BARE_FORMAT, "*", clientNonce);

        SaslInitialResponse saslInitialResponse = SaslInitialResponse
                .builder()
                .nameOfSaslAuthMechanism(PostgresProtocolAuthenticationMethod.SCRAM_SHA256.getProtocolMethodName())
                .saslMechanismSpecificData(PostgresProtocolScramConstants.GS2_HEADER + clientFirstMessageBare)
                .build();

        ByteBuf message = ClientPostgresProtocolMessageEncoder.encodeSaslInitialResponseMessage(saslInitialResponse);

        ctx.channel().writeAndFlush(message);
        authStatus = SaslAuthStatus.FIRST_CLIENT_MESSAGE_SENT;
        ctx.channel().read();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        if (SaslAuthStatus.FIRST_CLIENT_MESSAGE_SENT.equals(authStatus)) {

            AuthenticationSASLContinue saslContinue = ServerPostgresProtocolMessageDecoder.decodeAuthSaslContinueMessage(message);

            Pattern serverFirstMessagePattern = PostgresProtocolScramConstants.SERVER_FIRST_MESSAGE_PATTERN;
            Matcher serverFirstMessageMatcher = serverFirstMessagePattern.matcher(saslContinue.getSaslMechanismSpecificData());
            serverFirstMessageMatcher.matches();

            String serverNonce = serverFirstMessageMatcher.group(PostgresProtocolScramConstants.SERVER_FIRST_MESSAGE_SERVER_NONCE_MATCHER_GROUP);

            if (!serverNonce.startsWith(clientNonce)) {
                failConnectionAuth(ctx);
                return;
            }

            String g2HeaderEncoded = new String(Base64.getEncoder().encode(PostgresProtocolScramConstants.GS2_HEADER.getBytes()));

            String clientFinalMessageWithoutProof = String.format(
                    PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_WITHOUT_PROOF_FORMAT,
                    g2HeaderEncoded,
                    serverNonce
            );
            String authMessage = clientFirstMessageBare + "," + saslContinue.getSaslMechanismSpecificData() + "," + clientFinalMessageWithoutProof;

            byte[] clientKey = scramAuthInfo.getClientKey();
            byte[] storedKey = Base64.getDecoder().decode(scramAuthInfo.getStoredKeyBase64());

            byte[] clientSignature = ScramUtils.computeHmac(storedKey, PostgresProtocolScramConstants.SHA256_HMAC_NAME, authMessage);

            byte[] clientProof = clientKey.clone();
            for (int i = 0; i < clientProof.length; i++) {
                clientProof[i] ^= clientSignature[i];
            }

            String clientFinalMessage = String.format(
                    PostgresProtocolScramConstants.CLIENT_FINAL_MESSAGE_FORMAT,
                    g2HeaderEncoded,
                    serverNonce,
                    new String(Base64.getEncoder().encode(clientProof))
            );

            SaslResponse saslResponse = SaslResponse
                    .builder()
                    .saslMechanismSpecificData(clientFinalMessage)
                    .build();

            ByteBuf response = ClientPostgresProtocolMessageEncoder.encodeSaslResponseMessage(saslResponse);

            ctx.channel().writeAndFlush(response);
            authStatus = SaslAuthStatus.LAST_CLIENT_MESSAGE_SENT;

            ctx.channel().pipeline().addLast(
                    connectionPoolChannelHandlerProducer.createAfterAuthHandler(
                            callbackFunction
                    )
            );
            ctx.channel().pipeline().remove(this);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);
        failConnectionAuth(ctx);
    }

    private void failConnectionAuth(ChannelHandlerContext ctx) {
        HandlerUtils.closeOnFlush(ctx.channel());
        callbackFunction.accept(new PgChannelAuthResult(false));
    }
}
