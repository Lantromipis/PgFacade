package com.lantromipis.service.impl;

import com.lantromipis.handler.common.AbstractProxyClientHandler;
import com.lantromipis.service.api.InactiveClientConnectionsReaper;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
public class InactiveClientConnectionsReaperImpl implements InactiveClientConnectionsReaper {
    private List<AbstractProxyClientHandler> activeProxyHandlers = new CopyOnWriteArrayList<>();

    @Override
    public void registerNewProxyClientHandler(AbstractProxyClientHandler clientHandler) {

    }

    @Override
    public void unregisterProxyClientHandler(AbstractProxyClientHandler clientHandler) {

    }
}
