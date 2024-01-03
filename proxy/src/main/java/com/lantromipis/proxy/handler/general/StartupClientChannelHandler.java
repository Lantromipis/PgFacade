package com.lantromipis.proxy.handler.general;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.decoder.ClientPostgresProtocolMessageDecoder;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.internal.auth.PgAuthInfo;
import com.lantromipis.postgresprotocol.model.protocol.StartupMessage;
import com.lantromipis.postgresprotocol.utils.PostgresErrorMessageUtils;
import com.lantromipis.postgresprotocol.utils.PostgresHandlerUtils;
import com.lantromipis.proxy.auth.ProxyAuthProcessor;
import com.lantromipis.proxy.auth.ScramSha256AuthProcessor;
import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;
import com.lantromipis.proxy.producer.ProxyChannelHandlersProducer;
import com.lantromipis.usermanagement.model.UserAuthInfo;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StartupClientChannelHandler extends AbstractClientChannelHandler {

    private final UserAuthInfoProvider userAuthInfoProvider;
    private final ProxyChannelHandlersProducer proxyChannelHandlersProducer;
    private final boolean primaryMode;

    private String username;
    private StartupMessage startupMessage;
    private ProxyAuthProcessor proxyAuthProcessor;


    public StartupClientChannelHandler(final UserAuthInfoProvider userAuthInfoProvider,
                                       final ProxyChannelHandlersProducer proxyChannelHandlersProducer,
                                       final boolean primaryMode
    ) {
        this.userAuthInfoProvider = userAuthInfoProvider;
        this.proxyChannelHandlersProducer = proxyChannelHandlersProducer;
        this.primaryMode = primaryMode;
    }

    @Override
    public void forceDisconnectAndClearResources() {
        forceCloseConnectionWithEmptyError();
        setActive(false);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        @Cleanup("release") ByteBuf message = (ByteBuf) msg;

        if (startupMessage == null) {
            processMessageBeforeStartupMessage(ctx, message);
            ctx.channel().read();
        } else {
            PgAuthInfo pgAuthInfo = proxyAuthProcessor.processAuth(ctx, message);

            if (pgAuthInfo != null) {
                proxyChannelHandlersProducer.getNewSessionPooledConnectionHandlerByCallback(
                        startupMessage,
                        pgAuthInfo,
                        handler -> {
                            ctx.channel().pipeline().addLast(handler);
                            ctx.channel().pipeline().remove(this);
                            setActive(false);
                        },
                        primaryMode
                );
            } else {
                ctx.channel().read();
            }
        }

        //super.channelRead(ctx, msg);
    }

    private void processMessageBeforeStartupMessage(final ChannelHandlerContext ctx, ByteBuf message) {
        int firstInt = message.readInt();

        //means initial message was GSSENCRequest or SSLRequest
        if (firstInt == PostgresProtocolGeneralConstants.INITIAL_ENCRYPTION_REQUEST_MESSAGE_FIRST_INT) {
            ctx.channel().writeAndFlush(
                    ctx.alloc().buffer(1).writeBytes(PostgresProtocolGeneralConstants.ENCRYPTION_NOT_SUPPORTED_RESPONSE_MESSAGE)
            );
        } else {
            message.resetReaderIndex();
            startupMessage = ClientPostgresProtocolMessageDecoder.decodeStartupMessage(message);

            if (startupMessage == null) {
                log.error("Error decoding client startup message. Closing connection.");
                forceCloseConnectionWithEmptyError();
                return;
            }

            username = startupMessage.getParameters().get(PostgresProtocolGeneralConstants.STARTUP_PARAMETER_USER);

            if (startupMessage.getMajorVersion() != 3 && startupMessage.getMinorVersion() != 0) {
                log.error("User with role {} attempted to connect with wrong protocol version: major={}; minor={}. Closing connection.",
                        username,
                        startupMessage.getMajorVersion(),
                        startupMessage.getMinorVersion()
                );
                forceCloseConnectionWithEmptyError();
                return;
            }

            UserAuthInfo userAuthInfo = userAuthInfoProvider.getUserAuthInfo(username);

            if (userAuthInfo == null) {
                log.error("User with role {} not found or has unknown auth method. Closing connection.", username);
                forceCloseConnectionWithAuthError(username);
                return;
            }

            switch (userAuthInfo.getAuthenticationMethod()) {
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
                    proxyAuthProcessor = new ScramSha256AuthProcessor(username, userAuthInfo.getPasswd());
                }
                default -> forceCloseConnectionWithAuthError(username);
            }
        }
    }

    private void forceCloseConnectionWithAuthError(String username) {
        ChannelHandlerContext ctx = getInitialChannelHandlerContext();
        PostgresHandlerUtils.closeOnFlush(ctx.channel(), PostgresErrorMessageUtils.getAuthFailedForUserErrorMessage(username, ctx.alloc()));
        setActive(false);
    }

    private void forceCloseConnectionWithEmptyError() {
        ChannelHandlerContext ctx = getInitialChannelHandlerContext();
        PostgresHandlerUtils.closeOnFlush(ctx.channel(), ServerPostgresProtocolMessageEncoder.createEmptyErrorMessage(ctx.alloc()));
        setActive(false);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception while reading startup message!", cause);
        forceCloseConnectionWithAuthError(username == null ? "root" : username);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        setActive(false);
        super.channelInactive(ctx);
    }
}
