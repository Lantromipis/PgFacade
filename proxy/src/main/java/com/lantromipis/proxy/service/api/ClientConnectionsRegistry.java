package com.lantromipis.proxy.service.api;

import com.lantromipis.proxy.exception.ConnectionLimitReachedException;
import com.lantromipis.proxy.handler.proxy.client.AbstractDataProxyClientChannelHandler;

public interface ClientConnectionsRegistry {
    void registerNewProxyClientHandler(AbstractDataProxyClientChannelHandler clientHandler) throws ConnectionLimitReachedException;

    void unregisterProxyClientHandler(AbstractDataProxyClientChannelHandler clientHandler);

    boolean connectionsLimitReached();
}
