package com.lantromipis.proxy.handler.general;

import com.lantromipis.postgresprotocol.constant.PostgreSQLProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.decoder.clientPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.AuthenticationMethod;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StartupClientChannelHandler extends AbstractClientChannelHandler {

    private final UserAuthInfoProvider userAuthInfoProvider;
    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;


    public StartupClientChannelHandler(final UserAuthInfoProvider userAuthInfoProvider, final ProxyChannelHandlersProducer proxyChannelHandlersProducer) {
        this.userAuthInfoProvider = userAuthInfoProvider;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
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
            StartupMessage startupMessage = clientPostgresProtocolMessageDecoder.decodeStartupMessage(message);

            if (startupMessage == null) {
                log.error("Error decoding client startup message. Closing connection.");
                forceCloseConnectionWithError();
                return;
            }

            if (startupMessage.getMajorVersion() != 3 && startupMessage.getMinorVersion() != 0) {
                log.error("Client attempted to connect with wrong protocol version: major=" + startupMessage.getMajorVersion() + "; minor=" + startupMessage.getMinorVersion() + ". Closing connection.");
                forceCloseConnectionWithError();
                return;
            }

            AuthenticationMethod authenticationMethod = userAuthInfoProvider.getAuthMethodForUser(
                    startupMessage.getParameters().get(PostgreSQLProtocolGeneralConstants.STARTUP_PARAMETER_USER)
            );

            if (authenticationMethod == null) {
                log.error("User not found or has unknown authMethod. Closing connection.");
                forceCloseConnectionWithError();
                return;
            }

            ByteBuf authRequestMessage = ServerPostgresProtocolMessageEncoder.createAuthRequestMessage(authenticationMethod);

            switch (authenticationMethod) {
                case PLAIN_TEXT -> {
                    //TODO done
                    break;
                }
                case MD5 -> {
                    //TODO done
                    break;
                }
                case SCRAM_SHA256 -> {
                    ctx.channel().pipeline().addLast(
                            proxyChannelHandlersProducer.createNewSaslScramSha256AuthHandler(startupMessage)
                    );
                }
                default -> {
                    forceCloseConnectionWithError();
                    return;
                }
            }

            ctx.channel().pipeline().remove(this);
            setActive(false);

            ctx.channel().writeAndFlush(authRequestMessage);
            ctx.channel().read();
        }

        super.channelRead(ctx, msg);
    }
}
