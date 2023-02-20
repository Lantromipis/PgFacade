package com.lantromipis.proxy.handler.proxy;

import com.lantromipis.postgresprotocol.encoder.ServerPostgreSqlProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.UUID;

/**
 * Abstract Netty connection handler for all proxy connections.
 */
@Slf4j
public abstract class AbstractClientChannelHandler extends ChannelInboundHandlerAdapter {
    /**
     * UUID used for good and simple equals() and hashcode() implementations.
     */
    @Getter
    private final UUID equalsAndHashcodeId;

    /**
     * ChannelHandlerContext, must be captured when handler initialized.
     * This can be used, for example, for closing inactive connection.
     */
    @Getter
    @Setter
    private ChannelHandlerContext initialChannelHandlerContext;
    private long lastTimeAccessed = 0;
    /**
     * True if handler is working, false when not. For example, after handleInactivityPeriodEnded() called, this variable must become false.
     */
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private boolean active;

    public AbstractClientChannelHandler() {
        equalsAndHashcodeId = UUID.randomUUID();
    }

    /**
     * Used to retrieve last time when client was active. Active clients sends packets to PgFacade.
     *
     * @return last time when client was active
     */
    public long getLastActiveTimeMilliseconds() {
        return lastTimeAccessed;
    }

    /**
     * This method might be called when client was inactive for too long.
     * If this method is called, handler must close connection with client properly.
     */
    public void handleInactivityPeriodEnded() {
        rejectRequest(initialChannelHandlerContext);
        active = false;
    }

    /**
     * Auto-read is disabled, so we must read channel when handler is added.
     *
     * @param ctx channel handler context
     * @throws Exception when something wants wrong
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().read();
        initialize(ctx);
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        lastTimeAccessed = System.currentTimeMillis();
    }

    /**
     * Default handler for exceptions.
     *
     * @param ctx   channel handler context
     * @param cause throwable which occurred during connection handling
     * @throws Exception when something wants wrong
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in client connection handler. Connection will be closed ", cause);
        rejectRequest(ctx);
    }

    protected void rejectRequest(ChannelHandlerContext ctx) {
        ctx.channel().writeAndFlush(
                ServerPostgreSqlProtocolMessageEncoder.createEmptyErrorMessage()
        );
        HandlerUtils.closeOnFlush(ctx.channel());
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isActive()) {
            initialize(ctx);
        }
        super.channelRegistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        initialize(ctx);
        super.channelActive(ctx);
    }

    private void initialize(ChannelHandlerContext ctx) {
        initialChannelHandlerContext = ctx;
        active = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractClientChannelHandler that = (AbstractClientChannelHandler) o;
        return that.getEqualsAndHashcodeId().equals(equalsAndHashcodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(equalsAndHashcodeId);
    }
}
