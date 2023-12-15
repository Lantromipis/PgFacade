package com.lantromipis.postgresprotocol.handler.frontend.auth;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolScramConstants;
import com.lantromipis.postgresprotocol.decoder.ServerPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.handler.frontend.AbstractPgFrontendChannelHandler;
import com.lantromipis.postgresprotocol.model.internal.PgChannelAuthResult;
import com.lantromipis.postgresprotocol.model.internal.auth.ScramPgAuthInfo;
import com.lantromipis.postgresprotocol.model.protocol.AuthenticationSASLContinue;
import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import com.lantromipis.postgresprotocol.model.protocol.SaslInitialResponse;
import com.lantromipis.postgresprotocol.model.protocol.SaslResponse;
import com.lantromipis.postgresprotocol.producer.PgFrontendChannelHandlerProducer;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.postgresprotocol.utils.ScramUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class PgChannelSaslScramSha256AuthHandler extends AbstractPgFrontendChannelHandler {

    private enum SaslAuthStatus {
        NOT_STARTED,
        FIRST_CLIENT_MESSAGE_SENT,
        LAST_CLIENT_MESSAGE_SENT
    }

    private final String clientNonce;
    private String clientFirstMessageBare;

    private final PgFrontendChannelHandlerProducer connectionPoolChannelHandlerProducer;
    private final ScramPgAuthInfo scramAuthInfo;
    private final Consumer<PgChannelAuthResult> callbackFunction;

    private SaslAuthStatus authStatus = SaslAuthStatus.NOT_STARTED;

    public PgChannelSaslScramSha256AuthHandler(final PgFrontendChannelHandlerProducer connectionPoolChannelHandlerProducer,
                                               final ScramPgAuthInfo scramAuthInfo,
                                               final Consumer<PgChannelAuthResult> callbackFunction) {
        this.clientNonce = UUID.randomUUID().toString();

        this.connectionPoolChannelHandlerProducer = connectionPoolChannelHandlerProducer;
        this.scramAuthInfo = scramAuthInfo;
        this.callbackFunction = callbackFunction;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        clientFirstMessageBare = "n=*,r=" + clientNonce;

        SaslInitialResponse saslInitialResponse = SaslInitialResponse
                .builder()
                .nameOfSaslAuthMechanism(PostgresProtocolAuthenticationMethod.SCRAM_SHA256.getProtocolMethodName())
                .saslMechanismSpecificData(PostgresProtocolScramConstants.GS2_HEADER + clientFirstMessageBare)
                .build();

        ByteBuf message = ClientPostgresProtocolMessageEncoder.encodeSaslInitialResponseMessage(saslInitialResponse, ctx.alloc());

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

            String clientFinalMessageWithoutProof = "c=" + g2HeaderEncoded +
                    ",r=" + serverNonce;

            String authMessage = clientFirstMessageBare + "," + saslContinue.getSaslMechanismSpecificData() + "," + clientFinalMessageWithoutProof;
            byte[] authMessageBytes = authMessage.getBytes(StandardCharsets.US_ASCII);

            byte[] clientKey = scramAuthInfo.getClientKey();
            byte[] storedKey = Base64.getDecoder().decode(scramAuthInfo.getStoredKeyBase64());

            byte[] clientSignature = ScramUtils.computeHmac(storedKey, PostgresProtocolScramConstants.SHA256_HMAC_NAME, authMessageBytes);

            byte[] clientProof = clientKey.clone();
            for (int i = 0; i < clientProof.length; i++) {
                clientProof[i] ^= clientSignature[i];
            }

            String clientFinalMessage = "c=" + g2HeaderEncoded +
                    ",r=" + serverNonce +
                    ",p=" + new String(Base64.getEncoder().encode(clientProof));

            SaslResponse saslResponse = SaslResponse
                    .builder()
                    .saslMechanismSpecificData(clientFinalMessage)
                    .build();

            ByteBuf response = ClientPostgresProtocolMessageEncoder.encodeSaslResponseMessage(saslResponse, ctx.alloc());

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
