package com.lantromipis.proxy.service.impl;

import com.lantromipis.configuration.event.SwitchoverCompletedEvent;
import com.lantromipis.configuration.event.SwitchoverStartedEvent;
import com.lantromipis.configuration.properties.predefined.ProxyProperties;
import com.lantromipis.proxy.exception.ConnectionLimitReachedException;
import com.lantromipis.proxy.handler.proxy.AbstractClientChannelHandler;
import com.lantromipis.proxy.handler.proxy.client.AbstractDataProxyClientChannelHandler;
import com.lantromipis.proxy.service.api.ClientConnectionsManagementService;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@ApplicationScoped
public class ClientConnectionsManagementServiceImpl implements ClientConnectionsManagementService {

    @Inject
    ProxyProperties proxyProperties;

    private Set<AbstractClientChannelHandler> activeChannelsHandlers = ConcurrentHashMap.newKeySet();
    private long timeout;

    @PostConstruct
    public void init() {
        int setCapacity = (int) (proxyProperties.maxConnections() / 0.7);
        activeChannelsHandlers = ConcurrentHashMap.newKeySet(setCapacity);
        timeout = proxyProperties.inactiveClients().inactiveConnectionTimeout().toMillis();
    }

    @Override
    public void registerNewClientChannelHandler(AbstractClientChannelHandler handler) throws ConnectionLimitReachedException {
        if (activeChannelsHandlers.size() + 1 > proxyProperties.maxConnections()) {
            throw new ConnectionLimitReachedException("Can not register new connection because connection limit is reached.");
        }
        activeChannelsHandlers.add(handler);
    }

    @Override
    public void unregisterClientChannelHandler(AbstractClientChannelHandler handler) {
        activeChannelsHandlers.remove(handler);
    }

    @Override
    public void forceDisconnectAll() {
        for (AbstractClientChannelHandler client : activeChannelsHandlers) {
            client.forceDisconnect();
            unregisterClientChannelHandler(client);
        }
    }

    @Override
    public int getActiveClientsCount() {
        return activeChannelsHandlers.size();
    }

    @Override
    public boolean connectionsLimitReached() {
        return activeChannelsHandlers.size() > proxyProperties.maxConnections();
    }

    @Scheduled(every = "${pg-facade.proxy.inactive-clients.check-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void checkInactiveClients() {
        if (!proxyProperties.inactiveClients().disconnect()) {
            return;
        }

        long endTime = System.currentTimeMillis() - timeout;
        long inactiveCount = 0;
        for (AbstractClientChannelHandler client : activeChannelsHandlers) {
            if (!client.isActive()) {
                unregisterClientChannelHandler(client);
                continue;
            }

            boolean channelClosed = !Optional.ofNullable(client.getInitialChannelHandlerContext()).map(ctx -> ctx.channel().isActive()).orElse(true);
            if ((client.getLastActiveTimeMilliseconds() > 0 && client.getLastActiveTimeMilliseconds() < endTime) || channelClosed) {
                client.forceDisconnect();
                unregisterClientChannelHandler(client);
                inactiveCount++;
            }
        }

        if (inactiveCount > 0) {
            log.debug("Closed {} inactive connections.", inactiveCount);
        }
    }

    public void listenToSwitchoverStartedEvent(@Observes SwitchoverStartedEvent switchoverStartedEvent) {
        for (AbstractClientChannelHandler client : activeChannelsHandlers) {
            if (client instanceof AbstractDataProxyClientChannelHandler dataProxy && client.isActive()) {
                dataProxy.handleSwitchoverStarted();
            }
        }
    }

    public void listenToSwitchoverStartedEvent(@Observes SwitchoverCompletedEvent switchoverCompletedEvent) {
        for (AbstractClientChannelHandler client : activeChannelsHandlers) {
            if (client instanceof AbstractDataProxyClientChannelHandler dataProxy && client.isActive()) {
                dataProxy.handleSwitchoverCompleted(switchoverCompletedEvent.isSuccess());
            }
        }
    }
}
