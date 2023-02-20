package com.lantromipis.proxy.handler.proxy.client;

import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;

/**
 * Abstract Netty channel handler for proxying data from client connections to real Postgres.
 */
public abstract class AbstractDataProxyClientChannelHandler extends AbstractClientChannelHandler {
    public AbstractDataProxyClientChannelHandler() {
        super();
    }

    /**
     * This method is called when switchover started. After switchover completed this.handleSwitchoverCompleted() method will be called
     */
    public abstract void handleSwitchoverStarted();

    /**
     * This method is called when switchover completed.
     *
     * @param success true if switchover completed successfully
     */
    public abstract void handleSwitchoverCompleted(boolean success);
}
