package com.lantromipis.proxy.handler.proxy;

import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;
import java.util.UUID;

/**
 * Abstract Netty connection handler for all proxy connections. This class must be a superclass for all handlers, which are used by proxy.
 * When any child class object is created, such object must be registered in some connection manager. This allows PgFacade to track any client connections.
 */
public abstract class AbstractClientChannelHandler extends ChannelInboundHandlerAdapter {
    /**
     * UUID used for good and simple equals() and hashcode() implementations.
     */
    @Getter
    private final UUID equalsAndHashcodeId;

    /**
     * ChannelHandlerContext, must be captured when handler initialized.
     * This can be used, for example, for closing inactive connection.
     * Because this field exists, a new unique handler must be created for each channel
     */
    @Getter
    @Setter
    private ChannelHandlerContext initialChannelHandlerContext;

    /**
     * Contains timestamp when channel was last in-use. Can be used by reaper-service to close inactive connections.
     */
    private long lastTimeAccessed = 0;

    /**
     * True if handler is in use, false when not. For example, after forceDisconnect() called, this field must become false.
     * It is important to set this field to false when connection with client is closed, because special reaper-service uses this field.
     */
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private boolean active = true;

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
     * This method will be called when PgFacade needs the underlying client to be disconnected.
     * If this method is called, handler must close connection with client.
     * For example, this method is called when PgFacade is shutting down, or when client was inactive for too long.
     */
    public void forceDisconnect() {
        forceCloseConnectionWithEmptyError();
        active = false;
    }

    /**
     * Initializes custom handler and reads from channel. Auto-read is disabled by PgFacade design, so we must read channel when handler is added.
     *
     * @param ctx channel handler context
     * @throws Exception when something wants wrong
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        initialize(ctx);
        ctx.channel().read();
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        forceCloseConnectionWithEmptyError();
    }

    /**
     * Default method to call when connection must be closed. Closes connection with error.
     *
     * @param ctx ChannelHandlerContext of connection that will be closed.
     */
    protected void forceCloseConnectionWithEmptyError() {
        HandlerUtils.closeOnFlush(initialChannelHandlerContext.channel(), ServerPostgresProtocolMessageEncoder.createEmptyErrorMessage());
        active = false;
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
        lastTimeAccessed = System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractClientChannelHandler that = (AbstractClientChannelHandler) o;
        return that.getEqualsAndHashcodeId().equals(equalsAndHashcodeId);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        active = false;
        super.channelInactive(ctx);
    }

    @Override
    public int hashCode() {
        return Objects.hash(equalsAndHashcodeId);
    }
}
