package com.lantromipis.service.api;

import com.lantromipis.handler.common.AbstractProxyClientHandler;

public interface InactiveClientConnectionsReaper {
    void registerNewProxyClientHandler(AbstractProxyClientHandler clientHandler);

    void unregisterProxyClientHandler(AbstractProxyClientHandler clientHandler);
}
