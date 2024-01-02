package com.lantromipis.postgresprotocol.handler.frontend;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.decoder.ServerPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.PgChannelAuthResult;
import com.lantromipis.postgresprotocol.model.internal.auth.PgAuthInfo;
import com.lantromipis.postgresprotocol.model.internal.auth.ScramPgAuthInfo;
import com.lantromipis.postgresprotocol.model.protocol.AuthenticationRequestMessage;
import com.lantromipis.postgresprotocol.model.protocol.ErrorResponse;
import com.lantromipis.postgresprotocol.model.protocol.StartupMessage;
import com.lantromipis.postgresprotocol.producer.PgFrontendChannelHandlerProducer;
import com.lantromipis.postgresprotocol.utils.PostgresErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.PostgresHandlerUtils;
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

        byte messageMarker = message.readByte();
        message.readerIndex(0);
        switch (messageMarker) {
            case PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR -> {
                ErrorResponse errorResponse = ServerPostgresProtocolMessageDecoder.decodeErrorResponse(message);
                log.error("Failed to initiate new Postgres connection due to error response from server. Error from server: {}", PostgresErrorMessageUtils.getLoggableErrorMessageFromErrorResponse(errorResponse));
                closeConnection(ctx);
                return;
            }
            case PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR -> {
                // skip
            }
            default -> {
                log.error(
                        "Received unexpected message during new Postgres connection initiation. Expected {} or {}, but for {}!",
                        PostgresProtocolGeneralConstants.AUTH_REQUEST_START_CHAR,
                        PostgresProtocolGeneralConstants.ERROR_MESSAGE_START_CHAR,
                        messageMarker
                );
                closeConnection(ctx);
                return;
            }
        }

        AuthenticationRequestMessage authenticationRequestMessage = ServerPostgresProtocolMessageDecoder.decodeAuthRequestMessage(message);

        if (!Objects.equals(authenticationRequestMessage.getMethod(), pgAuthInfo.getExpectedAuthMethod())) {
            log.error("Can not create new Postgres connection. Expected auth method: " + pgAuthInfo.getExpectedAuthMethod() + " but actual auth method requested by Postgres '" + authenticationRequestMessage.getMethod() + "'");
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
        log.error("Exception in Postgres startup channel handler!", cause);
        closeConnection(ctx);
    }

    private void closeConnection(ChannelHandlerContext ctx) {
        callbackFunction.accept(new PgChannelAuthResult(false));
        PostgresHandlerUtils.closeOnFlush(ctx.channel());
    }
}
