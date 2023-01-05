package com.lantromipis.proxy.service.api;

import com.lantromipis.proxy.handler.common.AbstractProxyClientHandler;

public interface InactiveClientConnectionsReaper {
    void registerNewProxyClientHandler(AbstractProxyClientHandler clientHandler);

    void unregisterProxyClientHandler(AbstractProxyClientHandler clientHandler);
}
