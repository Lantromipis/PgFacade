package com.lantromipis.proxy.service.impl;

import com.lantromipis.proxy.handler.common.AbstractProxyClientHandler;
import com.lantromipis.proxy.service.api.InactiveClientConnectionsReaper;

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