package com.lantromipis.handler.auth;

import com.lantromipis.constant.PostgreSQLProtocolScramConstants;
import com.lantromipis.decoder.ServerPostgreSqlProtocolMessageDecoder;
import com.lantromipis.encoder.ClientPostgreSqlProtocolMessageEncoder;
import com.lantromipis.handler.common.AbstractConnectionPoolClientHandler;
import com.lantromipis.model.*;
import com.lantromipis.utils.HandlerUtils;
import com.lantromipis.utils.ScramUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Base64;
import java.util.UUID;
import java.util.function.Function;
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


    private ScramAuthInfo scramAuthInfo;
    private ConnectionInfo connectionInfo;
    private Function<Boolean, Void> callbackFunction;

    private SaslAuthStatus authStatus = SaslAuthStatus.NOT_STARTED;

    public PgChannelSaslScramSha256AuthHandler(ScramAuthInfo scramAuthInfo, ConnectionInfo connectionInfo, Function<Boolean, Void> callbackFunction) {
        this.clientNonce = UUID.randomUUID().toString();

        this.scramAuthInfo = scramAuthInfo;
        this.connectionInfo = connectionInfo;
        this.callbackFunction = callbackFunction;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        clientFirstMessageBare = String.format(PostgreSQLProtocolScramConstants.CLIENT_FIRST_MESSAGE_BARE_FORMAT, "*", clientNonce);

        SaslInitialResponse saslInitialResponse = SaslInitialResponse
                .builder()
                .nameOfSaslAuthMechanism(AuthenticationMethod.SCRAM_SHA256.getProtocolMethodName())
                .saslMechanismSpecificData(PostgreSQLProtocolScramConstants.GS2_HEADER + clientFirstMessageBare)
                .build();

        ByteBuf message = ClientPostgreSqlProtocolMessageEncoder.encodeSaslInitialResponseMessage(saslInitialResponse);

        ctx.channel().writeAndFlush(message);
        authStatus = SaslAuthStatus.FIRST_CLIENT_MESSAGE_SENT;
        ctx.channel().read();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        if (SaslAuthStatus.FIRST_CLIENT_MESSAGE_SENT.equals(authStatus)) {

            AuthenticationSASLContinue saslContinue = ServerPostgreSqlProtocolMessageDecoder.decodeAuthSaslContinueMessage(message);

            Pattern serverFirstMessagePattern = PostgreSQLProtocolScramConstants.SERVER_FIRST_MESSAGE_PATTERN;
            Matcher serverFirstMessageMatcher = serverFirstMessagePattern.matcher(saslContinue.getSaslMechanismSpecificData());
            serverFirstMessageMatcher.matches();

            String serverNonce = serverFirstMessageMatcher.group(PostgreSQLProtocolScramConstants.SERVER_FIRST_MESSAGE_SERVER_NONCE_MATCHER_GROUP);

            if (!serverNonce.startsWith(clientNonce)) {
                failConnectionAuth(ctx);
                return;
            }

            String salt = serverFirstMessageMatcher.group(PostgreSQLProtocolScramConstants.SERVER_FIRST_MESSAGE_SALT_MATCHER_GROUP);
            String iterationCount = serverFirstMessageMatcher.group(PostgreSQLProtocolScramConstants.SERVER_FIRST_MESSAGE_ITERATION_COUNT_MATCHER_GROUP);

            String g2HeaderEncoded = new String(Base64.getEncoder().encode(PostgreSQLProtocolScramConstants.GS2_HEADER.getBytes()));

            String clientFinalMessageWithoutProof = String.format(
                    PostgreSQLProtocolScramConstants.CLIENT_FINAL_MESSAGE_WITHOUT_PROOF_FORMAT,
                    g2HeaderEncoded,
                    serverNonce
            );
            String authMessage = clientFirstMessageBare + "," + saslContinue.getSaslMechanismSpecificData() + "," + clientFinalMessageWithoutProof;

            byte[] clientKey = scramAuthInfo.getClientKey();
            byte[] storedKey = Base64.getDecoder().decode(scramAuthInfo.getStoredKeyBase64());

            byte[] clientSignature = ScramUtils.computeHmac(storedKey, PostgreSQLProtocolScramConstants.SHA256_HMAC_NAME, authMessage);

            byte[] clientProof = clientKey.clone();
            for (int i = 0; i < clientProof.length; i++) {
                clientProof[i] ^= clientSignature[i];
            }

            String clientFinalMessage = String.format(
                    PostgreSQLProtocolScramConstants.CLIENT_FINAL_MESSAGE_FORMAT,
                    g2HeaderEncoded,
                    serverNonce,
                    new String(Base64.getEncoder().encode(clientProof))
            );

            SaslResponse saslResponse = SaslResponse
                    .builder()
                    .saslMechanismSpecificData(clientFinalMessage)
                    .build();

            ByteBuf response = ClientPostgreSqlProtocolMessageEncoder.encodeSaslResponseMessage(saslResponse);

            ctx.channel().writeAndFlush(response);
            authStatus = SaslAuthStatus.LAST_CLIENT_MESSAGE_SENT;
            ctx.channel().read();
        } else {
            //TODO check response OK
            ctx.channel().pipeline().remove(this);

            callbackFunction.apply(true);
            ctx.channel().read();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);
        failConnectionAuth(ctx);
    }

    private void failConnectionAuth(ChannelHandlerContext ctx) {
        HandlerUtils.closeOnFlush(ctx.channel());
        callbackFunction.apply(false);
    }
}
