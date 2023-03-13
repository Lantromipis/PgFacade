package com.lantromipis.proxy.handler.general;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.decoder.clientPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.PostgresProtocolAuthenticationMethod;
import com.lantromipis.postgresprotocol.model.StartupMessage;
import com.lantromipis.postgresprotocol.utils.ErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
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
        if (firstInt == PostgresProtocolGeneralConstants.INITIAL_ENCRYPTION_REQUEST_MESSAGE_FIRST_INT) {
            ctx.channel().writeAndFlush(
                    Unpooled.copiedBuffer(PostgresProtocolGeneralConstants.ENCRYPTION_NOT_SUPPORTED_RESPONSE_MESSAGE.getBytes())
            );
            ctx.channel().read();
        } else {
            message.resetReaderIndex();
            StartupMessage startupMessage = clientPostgresProtocolMessageDecoder.decodeStartupMessage(message);

            if (startupMessage == null) {
                log.error("Error decoding client startup message. Closing connection.");
                forceCloseConnectionWithEmptyError();
                return;
            }

            if (startupMessage.getMajorVersion() != 3 && startupMessage.getMinorVersion() != 0) {
                String username = startupMessage.getParameters().get(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER);
                log.error("User with role {} attempted to connect with wrong protocol version: major={}; minor={}. Closing connection.",
                        username,
                        startupMessage.getMajorVersion(),
                        startupMessage.getMinorVersion()
                );
                forceCloseConnectionWithEmptyError();
                return;
            }

            PostgresProtocolAuthenticationMethod postgresProtocolAuthenticationMethod = userAuthInfoProvider.getAuthMethodForUser(
                    startupMessage.getParameters().get(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER)
            );

            if (postgresProtocolAuthenticationMethod == null) {
                String username = startupMessage.getParameters().get(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER);
                log.error("User with role {} not found or has unknown auth method. Closing connection.", username);
                forceCloseConnectionWithAuthError(ctx, username);
                return;
            }

            switch (postgresProtocolAuthenticationMethod) {
                case PLAIN_TEXT -> {
                    //TODO done
                    break;
                }
                case MD5 -> {
                    //TODO done
                    break;
                }
                case SCRAM_SHA256 -> {
                    ctx.channel().writeAndFlush(ServerPostgresProtocolMessageEncoder.createAuthenticationSASLMessage());
                    ctx.channel().pipeline().addLast(
                            proxyChannelHandlersProducer.createNewSaslScramSha256AuthHandler(startupMessage)
                    );
                }
                default -> {
                    String username = startupMessage.getParameters().get(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER);
                    forceCloseConnectionWithAuthError(ctx, username);
                    return;
                }
            }

            ctx.channel().pipeline().remove(this);
            setActive(false);
        }

        super.channelRead(ctx, msg);
    }

    private void forceCloseConnectionWithAuthError(ChannelHandlerContext ctx, String username) {
        HandlerUtils.closeOnFlush(ctx.channel(), ErrorMessageUtils.getAuthFailedForUserErrorMessage(username));
    }
}
