package com.lantromipis.proxy.service.api;

import com.lantromipis.proxy.exception.ConnectionLimitReachedException;
import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;

public interface ClientConnectionsManagementService {
    void registerNewClientChannelHandler(AbstractClientChannelHandler handler) throws ConnectionLimitReachedException;

    void unregisterClientChannelHandler(AbstractClientChannelHandler handler);

    void forceDisconnectAll();

    int getActiveClientsCount();

    boolean connectionsLimitReached();
}
