package com.lantromipis.proxy.handler.general;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.decoder.ClientPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import com.lantromipis.postgresprotocol.model.protocol.StartupMessage;
import com.lantromipis.postgresprotocol.utils.ErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StartupClientChannelHandler extends AbstractClientChannelHandler {

    private final UserAuthInfoProvider userAuthInfoProvider;
    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;

    private String username;


    public StartupClientChannelHandler(final UserAuthInfoProvider userAuthInfoProvider, final ProxyChannelHandlersProducer proxyChannelHandlersProducer) {
        this.userAuthInfoProvider = userAuthInfoProvider;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
    }

    @Override
    public void forceDisconnectAndClearResources() {
        forceCloseConnectionWithEmptyError();
        setActive(false);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        int firstInt = message.readInt();

        //means initial message was GSSENCRequest or SSLRequest
        if (firstInt == PostgresProtocolGeneralConstants.INITIAL_ENCRYPTION_REQUEST_MESSAGE_FIRST_INT) {
            ctx.channel().writeAndFlush(
                    ctx.alloc().buffer(1).writeBytes(PostgresProtocolGeneralConstants.ENCRYPTION_NOT_SUPPORTED_RESPONSE_MESSAGE)
            );
            ctx.channel().read();
        } else {
            message.resetReaderIndex();
            StartupMessage startupMessage = ClientPostgresProtocolMessageDecoder.decodeStartupMessage(message);

            if (startupMessage == null) {
                log.error("Error decoding client startup message. Closing connection.");
                forceCloseConnectionWithEmptyError();
                return;
            }

            if (startupMessage.getMajorVersion() != 3 && startupMessage.getMinorVersion() != 0) {
                username = startupMessage.getParameters().get(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER);
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
                username = startupMessage.getParameters().get(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER);
                log.error("User with role {} not found or has unknown auth method. Closing connection.", username);
                forceCloseConnectionWithAuthError(username);
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
                    ctx.channel().writeAndFlush(ServerPostgresProtocolMessageEncoder.createAuthenticationSASLMessage(ctx.alloc()));
                    ctx.channel().pipeline().addLast(
                            proxyChannelHandlersProducer.createNewSaslScramSha256AuthHandler(startupMessage)
                    );
                }
                default -> {
                    String username = startupMessage.getParameters().get(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER);
                    forceCloseConnectionWithAuthError(username);
                    return;
                }
            }

            ctx.channel().pipeline().remove(this);
            setActive(false);
        }

        super.channelRead(ctx, msg);
    }

    private void forceCloseConnectionWithAuthError(String username) {
        HandlerUtils.closeOnFlush(getInitialChannelHandlerContext().channel(), ErrorMessageUtils.getAuthFailedForUserErrorMessage(username));
        setActive(false);
    }

    private void forceCloseConnectionWithEmptyError() {
        HandlerUtils.closeOnFlush(getInitialChannelHandlerContext().channel(), ServerPostgresProtocolMessageEncoder.createEmptyErrorMessage());
        setActive(false);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception while reading startup message!", cause);
        forceCloseConnectionWithAuthError(username == null ? "root" : username);
    }
}
