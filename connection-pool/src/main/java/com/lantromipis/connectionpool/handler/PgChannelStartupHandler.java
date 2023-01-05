package com.lantromipis.connectionpool.handler;

import com.lantromipis.connectionpool.handler.common.AbstractConnectionPoolClientHandler;
import com.lantromipis.connectionpool.model.ConnectionInfo;
import com.lantromipis.connectionpool.model.ScramAuthInfo;
import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.postgresprotocol.decoder.ServerPostgreSqlProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgreSqlProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.AuthenticationRequestMessage;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.function.Function;

@Slf4j
public class PgChannelStartupHandler extends AbstractConnectionPoolClientHandler {

    private ConnectionPoolChannelHandlerProducer connectionPoolChannelHandlerProducer;
    private AuthAdditionalInfo authAdditionalInfo;
    private ConnectionInfo connectionInfo;
    private Function<Boolean, Void> callbackFunction;

    public PgChannelStartupHandler(final ConnectionPoolChannelHandlerProducer connectionPoolChannelHandlerProducer,
                                   final AuthAdditionalInfo authAdditionalInfo,
                                   final ConnectionInfo connectionInfo,
                                   final Function<Boolean, Void> callbackFunction) {
        this.authAdditionalInfo = authAdditionalInfo;
        this.connectionPoolChannelHandlerProducer = connectionPoolChannelHandlerProducer;
        this.connectionInfo = connectionInfo;
        this.callbackFunction = callbackFunction;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        StartupMessage startupMessage = StartupMessage
                .builder()
                .majorVersion((short) 3)
                .minorVersion((short) 0)
                .parameters(connectionInfo.getParameters())
                .build();

        ByteBuf buf = ClientPostgreSqlProtocolMessageEncoder.encodeClientStartupMessage(startupMessage);

        ctx.channel().writeAndFlush(buf);
        ctx.channel().read();
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        AuthenticationRequestMessage authenticationRequestMessage = ServerPostgreSqlProtocolMessageDecoder.decodeAuthRequestMessage(message);

        if (!Objects.equals(authenticationRequestMessage.getMethod(), authAdditionalInfo.getExpectedAuthMethod())) {
            log.error("Can not create pooled connection. Expected auth method: " + authAdditionalInfo.getExpectedAuthMethod() + " but actual auth method requested by Postgres '" + authenticationRequestMessage.getMethod() + "'");
            closeConnection(ctx);
            return;
        }

        ctx.channel().pipeline().addLast(
                connectionPoolChannelHandlerProducer.createNewSaslScramSha256AuthHandler(
                        (ScramAuthInfo) authAdditionalInfo,
                        connectionInfo,
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
        callbackFunction.apply(false);
        HandlerUtils.closeOnFlush(ctx.channel());
    }
}
