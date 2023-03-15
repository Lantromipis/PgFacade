package com.lantromipis.connectionpool.model;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Storage class for connection pool to help manage connection which belong to one Postgres instance.
 */
public class PostgresInstancePooledConnectionsStorage {

    @Setter
    @Getter
    private int maxConnections;
    private ConcurrentHashMap<StartupMessageInfo, ConcurrentLinkedDeque<PooledConnectionInternalInfo>> allConnections;
    private ConcurrentHashMap<StartupMessageInfo, ConcurrentLinkedDeque<PooledConnectionInternalInfo>> freeConnections;
    private AtomicInteger connectionsCount;

    public PostgresInstancePooledConnectionsStorage(int maxConnections) {
        this.maxConnections = maxConnections;
        allConnections = new ConcurrentHashMap<>();
        freeConnections = new ConcurrentHashMap<>();
        connectionsCount = new AtomicInteger(0);
    }

    /**
     * This method allows to check if storage is full and or not.
     * If not full, then internal storage counter will be incremented, which allows safely adding new connection sometime in the future without check if storage is full.
     * Main purpose to increase performance: first reserve place, then, if reservation was successful, create new connection with Postgres.
     *
     * @return true if reserved place and false if not
     */
    public boolean reserveSpaceForNewChannel() {
        while (true) {
            int value = connectionsCount.get();
            if (value >= maxConnections) {
                return false;
            }
            if (connectionsCount.compareAndSet(value, value + 1)) {
                return true;
            }
        }
    }

    /**
     * This method simply decreases the internal storage counter.
     * Must always be called after reserveSpaceForNewChannel(), so storage counter won't be broken.
     */
    public void cancelReservation() {
        connectionsCount.decrementAndGet();
    }

    /**
     * Adds new connection to storage and marks it as free. Space for connection must already be reserved.
     *
     * @param startupMessageInfo info about connection that was received from client in startup message
     * @param channel            representation of real connection to primary
     * @return a new pooled connection marked as 'free'
     */
    public PooledConnectionInternalInfo addNewChannel(StartupMessageInfo startupMessageInfo, Channel channel, byte[] serverParameterMessagesBytes) {
        long timestamp = System.currentTimeMillis();
        PooledConnectionInternalInfo pooledConnectionInternalInfo = PooledConnectionInternalInfo
                .builder()
                .startupMessageInfo(startupMessageInfo)
                .taken(new AtomicBoolean(false))
                .lastFreeTimestamp(timestamp)
                .createdTimestamp(timestamp)
                .realPostgresConnection(channel)
                .serverParameters(serverParameterMessagesBytes)
                .build();

        addConnectionToMap(startupMessageInfo, pooledConnectionInternalInfo, allConnections);
        addConnectionToMap(startupMessageInfo, pooledConnectionInternalInfo, freeConnections);

        return pooledConnectionInternalInfo;
    }

    /**
     * Adds new connection to storage and marks it as taken. Space for connection must already be reserved.
     *
     * @param startupMessageInfo info about connection that was received from client in startup message
     * @param channel            representation of real connection to primary
     * @return a new pooled connection marked as 'taken'
     */
    public PooledConnectionInternalInfo addNewChannelAndMarkAsTaken(StartupMessageInfo startupMessageInfo, Channel channel, byte[] serverParameterMessagesBytes) {
        long timestamp = System.currentTimeMillis();
        PooledConnectionInternalInfo pooledConnectionInternalInfo = PooledConnectionInternalInfo
                .builder()
                .startupMessageInfo(startupMessageInfo)
                .taken(new AtomicBoolean(true))
                .lastFreeTimestamp(timestamp)
                .createdTimestamp(timestamp)
                .realPostgresConnection(channel)
                .serverParameters(serverParameterMessagesBytes)
                .build();

        addConnectionToMap(startupMessageInfo, pooledConnectionInternalInfo, allConnections);

        return pooledConnectionInternalInfo;
    }

    /**
     * If connection has not reached its max-age, then this method returns connection back to storage and marks it as 'free'. Such connection must have been added earlier and marked as 'taken'.
     * If connection has reached its max-age, then such connection is removed from storage.
     * There are NO checks if connection was really added previously or marked as 'taken'! Checks are not included to improve performance.
     *
     * @param pooledConnectionInternalInfo polled connection that was taken from the pool
     * @param maxAgeMillis                 max age of this connection in milliseconds
     * @return Netty channel if connection reached it max-age or null if it doesn't.
     */
    public Channel returnTakenConnectionAndCheckAge(PooledConnectionInternalInfo pooledConnectionInternalInfo, long maxAgeMillis) {
        if (System.currentTimeMillis() - maxAgeMillis > pooledConnectionInternalInfo.getCreatedTimestamp()) {
            removeConnection(pooledConnectionInternalInfo);
            return pooledConnectionInternalInfo.getRealPostgresConnection();
        }

        pooledConnectionInternalInfo.setLastFreeTimestamp(System.currentTimeMillis());
        pooledConnectionInternalInfo.getTaken().set(false);
        addConnectionToMap(pooledConnectionInternalInfo.getStartupMessageInfo(), pooledConnectionInternalInfo, freeConnections);
        return null;
    }

    /**
     * Finds and returns free and open connection based on provided startup info. Returned connection will be marked as 'taken'.
     *
     * @param startupMessageInfo criteria to search for connection
     * @return PooledConnection marked as 'taken' or null if no connection matching criteria was found.
     */
    public PooledConnectionInternalInfo getFreeConnection(StartupMessageInfo startupMessageInfo) {
        ConcurrentLinkedDeque<PooledConnectionInternalInfo> deque = freeConnections.get(startupMessageInfo);

        if (deque == null) {
            return null;
        }

        PooledConnectionInternalInfo pooledConnectionInternalInfo = deque.pollFirst();

        if (pooledConnectionInternalInfo == null) {
            return null;
        }

        // mark connection as taken to prevent it from being removed if cleaner is working concurrently now
        if (pooledConnectionInternalInfo.getRealPostgresConnection().isActive() && pooledConnectionInternalInfo.getTaken().compareAndSet(false, true)) {
            return pooledConnectionInternalInfo;
        }

        // corner case: taken connection was removed by cleaner or was closed by postgres. No need to return it to queue, it will be removed anyway.
        return getFreeConnection(startupMessageInfo);
    }

    /**
     * Removes unneeded or closed connections that was not in use for some period of time. Only this method can really remove connection from storage.
     * Only connections marked as 'free' are removed.
     * Concurrent calls of this method is forbidden!
     *
     * @param lastTimestamp identifying the oldest timestamp for connection which will not be removed
     * @return list of freed connections
     */
    public List<Channel> removeRedundantConnections(long lastTimestamp, long maxAgeMillis) {
        List<Channel> freedChannels = freeConnections.values()
                .stream()
                .flatMap(Collection::stream)
                .filter(connection -> {
                    // check if connection is old enough to be removed.
                    // if old enough, then mark it as 'taken' to prevent it from being taken by client
                    if (connection.getLastFreeTimestamp() < lastTimestamp && connection.getTaken().compareAndSet(false, true)) {
                        removeConnection(connection);
                        return true;
                    }

                    if (System.currentTimeMillis() - maxAgeMillis > connection.getCreatedTimestamp() && connection.getTaken().compareAndSet(false, true)) {
                        removeConnection(connection);
                        return true;
                    }

                    return false;
                })
                .map(PooledConnectionInternalInfo::getRealPostgresConnection)
                .collect(Collectors.toList());

        // remove all 'dead' connections
        freedChannels.addAll(
                allConnections.values()
                        .stream()
                        .flatMap(Collection::stream)
                        .filter(connection -> {
                            // check if connection is closed
                            if (!connection.getRealPostgresConnection().isActive()) {
                                removeConnection(connection);
                                return true;
                            }

                            return false;
                        })
                        .map(PooledConnectionInternalInfo::getRealPostgresConnection)
                        .toList()
        );

        return freedChannels;
    }

    private void addConnectionToMap(StartupMessageInfo startupMessageInfo, PooledConnectionInternalInfo connection, ConcurrentHashMap<StartupMessageInfo, ConcurrentLinkedDeque<PooledConnectionInternalInfo>> map) {
        map.compute(startupMessageInfo, (k, v) -> {
            if (v == null) {
                var queue = new ConcurrentLinkedDeque<PooledConnectionInternalInfo>();
                queue.addFirst(connection);
                return queue;
            } else {
                v.addFirst(connection);
                return v;
            }
        });
    }

    private void removeConnection(PooledConnectionInternalInfo connection) {
        StartupMessageInfo startupMessageInfo = connection.getStartupMessageInfo();

        Optional.ofNullable(freeConnections.get(startupMessageInfo)).ifPresent(queue -> queue.remove(connection));
        Optional.ofNullable(allConnections.get(startupMessageInfo)).ifPresent(queue -> {
            if (queue.remove(connection)) {
                connectionsCount.decrementAndGet();
            }
        });
    }
}
