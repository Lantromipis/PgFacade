package com.lantromipis.connectionpool.handler.common;

import com.lantromipis.connectionpool.handler.ConnectionPoolChannelHandlerProducer;
import com.lantromipis.connectionpool.model.PgChannelAuthResult;
import com.lantromipis.connectionpool.model.StartupMessageInfo;
import com.lantromipis.connectionpool.model.auth.ScramAuthInfo;
import com.lantromipis.connectionpool.model.auth.AuthAdditionalInfo;
import com.lantromipis.postgresprotocol.decoder.ServerPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.protocol.AuthenticationRequestMessage;
import com.lantromipis.postgresprotocol.model.protocol.StartupMessage;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class PgChannelStartupHandler extends AbstractConnectionPoolClientHandler {

    private ConnectionPoolChannelHandlerProducer connectionPoolChannelHandlerProducer;
    private AuthAdditionalInfo authAdditionalInfo;
    private StartupMessageInfo startupMessageInfo;
    private Consumer<PgChannelAuthResult> callbackFunction;

    public PgChannelStartupHandler(final ConnectionPoolChannelHandlerProducer connectionPoolChannelHandlerProducer,
                                   final AuthAdditionalInfo authAdditionalInfo,
                                   final StartupMessageInfo startupMessageInfo,
                                   final Consumer<PgChannelAuthResult> callbackFunction) {
        this.authAdditionalInfo = authAdditionalInfo;
        this.connectionPoolChannelHandlerProducer = connectionPoolChannelHandlerProducer;
        this.startupMessageInfo = startupMessageInfo;
        this.callbackFunction = callbackFunction;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        StartupMessage startupMessage = StartupMessage
                .builder()
                .majorVersion((short) 3)
                .minorVersion((short) 0)
                .parameters(startupMessageInfo.getParameters())
                .build();

        ByteBuf buf = ClientPostgresProtocolMessageEncoder.encodeClientStartupMessage(startupMessage);

        ctx.channel().writeAndFlush(buf);
        ctx.channel().read();
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        AuthenticationRequestMessage authenticationRequestMessage = ServerPostgresProtocolMessageDecoder.decodeAuthRequestMessage(message);

        if (!Objects.equals(authenticationRequestMessage.getMethod(), authAdditionalInfo.getExpectedAuthMethod())) {
            log.error("Can not create pooled connection. Expected auth method: " + authAdditionalInfo.getExpectedAuthMethod() + " but actual auth method requested by Postgres '" + authenticationRequestMessage.getMethod() + "'");
            closeConnection(ctx);
            return;
        }

        ctx.channel().pipeline().addLast(
                connectionPoolChannelHandlerProducer.createNewSaslScramSha256AuthHandler(
                        (ScramAuthInfo) authAdditionalInfo,
                        startupMessageInfo,
                        callbackFunction
                )
        );

        ctx.channel().pipeline().remove(this);

        ctx.channel().read();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);
        closeConnection(ctx);
    }

    private void closeConnection(ChannelHandlerContext ctx) {
        callbackFunction.accept(new PgChannelAuthResult(false));
        HandlerUtils.closeOnFlush(ctx.channel());
    }
}
