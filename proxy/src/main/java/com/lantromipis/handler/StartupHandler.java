package com.lantromipis.handler;

import com.lantromipis.decoder.ClientPostgreSqlProtocolMessageDecoder;
import com.lantromipis.encoder.ServerPostgreSqlProtocolMessageEncoder;
import com.lantromipis.handler.common.AbstractHandler;
import com.lantromipis.model.AuthenticationMethod;
import com.lantromipis.model.StartupMessage;
import com.lantromipis.producer.ProxyChannelHandlersProducer;
import com.lantromipis.provider.api.UserAuthInfoProvider;
import com.lantromipis.constant.PostgreSQLProtocolGeneralConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StartupHandler extends AbstractHandler {

    private final UserAuthInfoProvider userAuthInfoProvider;
    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;


    public StartupHandler(final UserAuthInfoProvider userAuthInfoProvider, final ProxyChannelHandlersProducer proxyChannelHandlersProducer) {
        this.userAuthInfoProvider = userAuthInfoProvider;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        ByteBuf message = (ByteBuf) msg;

        int firstInt = message.readInt();

        //means initial message was GSSENCRequest or SSLRequest
        if (firstInt == PostgreSQLProtocolGeneralConstants.INITIAL_ENCRYPTION_REQUEST_MESSAGE_FIRST_INT) {
            ctx.channel().writeAndFlush(
                    Unpooled.copiedBuffer(PostgreSQLProtocolGeneralConstants.ENCRYPTION_NOT_SUPPORTED_RESPONSE_MESSAGE.getBytes())
            );
            ctx.channel().read();
        } else {
            message.resetReaderIndex();
            StartupMessage startupMessage = ClientPostgreSqlProtocolMessageDecoder.decodeStartupMessage(message);

            if (startupMessage == null) {
                log.error("Error decoding client startup message. Closing connection.");
                rejectRequest(ctx);
                return;
            }

            if (startupMessage.getMajorVersion() != 3 && startupMessage.getMinorVersion() != 0) {
                log.error("Client attempted to connect with wrong protocol version: major=" + startupMessage.getMajorVersion() + "; minor=" + startupMessage.getMinorVersion() + ". Closing connection.");
                rejectRequest(ctx);
                return;
            }

            AuthenticationMethod authenticationMethod = userAuthInfoProvider.getAuthMethodForUser(
                    startupMessage.getParameters().get(PostgreSQLProtocolGeneralConstants.STARTUP_PARAMETER_USER)
            );

            if (authenticationMethod == null) {
                log.error("User not found or has unknown authMethod. Closing connection.");
                rejectRequest(ctx);
                return;
            }

            ByteBuf authRequestMessage = ServerPostgreSqlProtocolMessageEncoder.createAuthRequestMessage(authenticationMethod);

            switch (authenticationMethod) {
                case PLAIN_TEXT -> {
                    break;
                }
                case MD5 -> {
                    break;
                }
                case SCRAM_SHA256 -> {
                    ctx.channel().pipeline().addLast(
                            proxyChannelHandlersProducer.createNewSaslScramSha256AuthHandler(startupMessage)
                    );
                    ctx.channel().pipeline().remove(this);
                }
                default -> {
                    rejectRequest(ctx);
                    return;
                }
            }

            ctx.channel().writeAndFlush(authRequestMessage);
            ctx.channel().read();
        }
    }
}
