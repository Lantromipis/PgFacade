package com.lantromipis.postgresprotocol.handler.frontend;

import com.lantromipis.postgresprotocol.decoder.ServerPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.PgChannelAuthResult;
import com.lantromipis.postgresprotocol.model.internal.auth.PgAuthInfo;
import com.lantromipis.postgresprotocol.model.internal.auth.ScramPgAuthInfo;
import com.lantromipis.postgresprotocol.model.protocol.AuthenticationRequestMessage;
import com.lantromipis.postgresprotocol.model.protocol.StartupMessage;
import com.lantromipis.postgresprotocol.producer.PgFrontendChannelHandlerProducer;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

@Slf4j
public class PgChannelStartupHandler extends AbstractPgFrontendChannelHandler {

    private final PgFrontendChannelHandlerProducer connectionPoolChannelHandlerProducer;
    private final PgAuthInfo pgAuthInfo;
    private final Map<String, String> parameters;
    private final Consumer<PgChannelAuthResult> callbackFunction;

    public PgChannelStartupHandler(final PgFrontendChannelHandlerProducer connectionPoolChannelHandlerProducer,
                                   final PgAuthInfo pgAuthInfo,
                                   final Map<String, String> startupParameters,
                                   final Consumer<PgChannelAuthResult> callbackFunction) {
        this.pgAuthInfo = pgAuthInfo;
        this.connectionPoolChannelHandlerProducer = connectionPoolChannelHandlerProducer;
        this.parameters = startupParameters;
        this.callbackFunction = callbackFunction;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        StartupMessage startupMessage = StartupMessage
                .builder()
                .majorVersion((short) 3)
                .minorVersion((short) 0)
                .parameters(parameters)
                .build();

        ByteBuf buf = ClientPostgresProtocolMessageEncoder.encodeClientStartupMessage(startupMessage, ctx.alloc());

        ctx.channel().writeAndFlush(buf);
        ctx.channel().read();
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        AuthenticationRequestMessage authenticationRequestMessage = ServerPostgresProtocolMessageDecoder.decodeAuthRequestMessage(message);

        if (!Objects.equals(authenticationRequestMessage.getMethod(), pgAuthInfo.getExpectedAuthMethod())) {
            log.error("Can not create new Postgres connection. Expected auth method: " + pgAuthInfo.getExpectedAuthMethod() + " but actual auth method requested by Postgres '" + authenticationRequestMessage.getMethod() + "'");
            callbackFunction.accept(new PgChannelAuthResult(false));
            closeConnection(ctx);
            return;
        }

        ctx.channel().pipeline().addLast(
                connectionPoolChannelHandlerProducer.createNewSaslScramSha256AuthHandler(
                        (ScramPgAuthInfo) pgAuthInfo,
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
