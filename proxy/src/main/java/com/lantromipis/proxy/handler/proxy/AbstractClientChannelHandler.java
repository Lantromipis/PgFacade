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
     * If fields is false, then all resources used by handler must already be closed and cleaned!
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
     * This method will be called when PgFacade unregisters handler.
     * If this method is called, handler must close connection with client AND clear or return all used resources.
     * This method might be called if Netty channel is already closed, but resources must be freed anyway.
     * Also, this method might be called multiple times. Handler implementation must be ready for that and track if resources are already freed. For example, no need to return connection to pool multiple times.
     */
    public abstract void forceDisconnectAndClearResources();

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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        forceDisconnectAndClearResources();
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

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        forceDisconnectAndClearResources();
        active = false;
        super.channelInactive(ctx);
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

    protected void initialize(ChannelHandlerContext ctx) {
        initialChannelHandlerContext = ctx;
        active = true;
        lastTimeAccessed = System.currentTimeMillis();
    }
}
