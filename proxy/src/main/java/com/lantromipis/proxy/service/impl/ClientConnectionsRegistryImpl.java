package com.lantromipis.proxy.service.impl;

import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.properties.predefined.ProxyStaticProperties;
import com.lantromipis.proxy.exception.ConnectionLimitReachedException;
import com.lantromipis.proxy.handler.proxy.client.AbstractDataProxyClientChannelHandler;
import com.lantromipis.proxy.service.api.ClientConnectionsRegistry;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@ApplicationScoped
public class ClientConnectionsRegistryImpl implements ClientConnectionsRegistry {

    @Inject
    ProxyStaticProperties proxyStaticProperties;

    @Inject
    Event<SwitchoverStartedEvent> switchoverStartedEvent;

    @Inject
    Event<SwitchoverCompletedEvent> switchoverCompletedEvent;

    private Set<AbstractDataProxyClientChannelHandler> activeProxyHandlers = ConcurrentHashMap.newKeySet();
    private long timeout;

    @PostConstruct
    public void init() {
        int setCapacity = (int) (proxyStaticProperties.maxConnections() / 0.7);
        activeProxyHandlers = ConcurrentHashMap.newKeySet(setCapacity);
        timeout = proxyStaticProperties.inactiveClients().inactiveConnectionTimeout().toMillis();
    }

    @Override
    public void registerNewProxyClientHandler(AbstractDataProxyClientChannelHandler clientHandler) throws ConnectionLimitReachedException {
        if (activeProxyHandlers.size() + 1 > proxyStaticProperties.maxConnections()) {
            throw new ConnectionLimitReachedException("Can not register new connection because connection limit is reached.");
        }
        activeProxyHandlers.add(clientHandler);
    }

    @Override
    public void unregisterProxyClientHandler(AbstractDataProxyClientChannelHandler clientHandler) {
        activeProxyHandlers.remove(clientHandler);
    }

    @Override
    public boolean connectionsLimitReached() {
        return activeProxyHandlers.size() > proxyStaticProperties.maxConnections();
    }

    @Scheduled(every = "${pg-facade.proxy.inactive-clients.check-interval}")
    public void checkInactiveClients() {
        if (!proxyStaticProperties.inactiveClients().disconnect()) {
            return;
        }

        long endTime = System.currentTimeMillis() - timeout;
        long inactiveCount = 0;
        for (AbstractDataProxyClientChannelHandler client : activeProxyHandlers) {
            if (client.getLastActiveTimeMilliseconds() > 0 && client.getLastActiveTimeMilliseconds() < endTime) {
                client.handleInactivityPeriodEnded();
                unregisterProxyClientHandler(client);
                inactiveCount++;
            }
        }

        if (inactiveCount > 0) {
            log.info("Closed {} inactive connections.", inactiveCount);
        }
    }
}
