package com.lantromipis.handler;

import com.lantromipis.decoder.ServerPostgreSqlProtocolMessageDecoder;
import com.lantromipis.encoder.ClientPostgreSqlProtocolMessageEncoder;
import com.lantromipis.handler.common.AbstractConnectionPoolClientHandler;
import com.lantromipis.model.AuthenticationRequestMessage;
import com.lantromipis.model.ConnectionInfo;
import com.lantromipis.model.ScramAuthInfo;
import com.lantromipis.model.StartupMessage;
import com.lantromipis.model.common.AuthAdditionalInfo;
import com.lantromipis.utils.HandlerUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.function.Function;

@Slf4j
public class ChannelStartupHandler extends AbstractConnectionPoolClientHandler {

    private ConnectionPoolChannelHandlerProducer connectionPoolChannelHandlerProducer;
    private AuthAdditionalInfo authAdditionalInfo;
    private ConnectionInfo connectionInfo;
    private Function<Boolean, Void> callbackFunction;

    public ChannelStartupHandler(final ConnectionPoolChannelHandlerProducer connectionPoolChannelHandlerProducer,
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
            HandlerUtils.closeOnFlush(ctx.channel());
            //TODO close connection properly
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
        callbackFunction.apply(false);
        log.error(cause.getMessage(), cause);
        HandlerUtils.closeOnFlush(ctx.channel());
    }
}
