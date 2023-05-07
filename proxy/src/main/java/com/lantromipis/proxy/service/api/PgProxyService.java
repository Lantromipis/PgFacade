package com.lantromipis.proxy.service.api;

import java.time.Duration;

public interface PgProxyService {
    void initialize();

    /**
     * Method is called to stop proxy. As soon as it called proxy must stop accepting connections.
     * If awaitClients is true, then method will not return until all clients are disconnected or timeout defined by awaitClientsDuration reached. After timout reached all clients will be disconnected forcibly.
     * If awaitClients is false, then all clients are disconnected forcibly and method returns.
     *
     * @param awaitClients         if true waits for clients to disconnect. If false, disconnect all clients forcibly.
     * @param awaitClientsDuration defines how long method should wait for clients to disconnect.
     */
    void shutdown(boolean awaitClients, Duration awaitClientsDuration);
}
