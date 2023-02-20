package com.lantromipis.proxy.service.api;

import com.lantromipis.proxy.exception.ConnectionLimitReachedException;
import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;
import com.lantromipis.proxy.handler.proxy.client.AbstractDataProxyClientChannelHandler;

public interface ClientConnectionsRegistry {
    void registerNewClientChannelHandler(AbstractClientChannelHandler handler) throws ConnectionLimitReachedException;

    void unregisterClientChannelHandler(AbstractClientChannelHandler handler);

    boolean connectionsLimitReached();
}
