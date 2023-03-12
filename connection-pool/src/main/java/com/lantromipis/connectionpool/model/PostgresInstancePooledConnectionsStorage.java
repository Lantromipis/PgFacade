package com.lantromipis.connectionpool.model;

import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Storage class for connection pool to help manage connection which belong to one Postgres instance.
 */
public class PostgresInstancePooledConnectionsStorage {
    private ConcurrentHashMap<StartupMessageInfo, ConcurrentLinkedDeque<PooledConnectionInternalInfo>> allConnections;
    private ConcurrentHashMap<StartupMessageInfo, ConcurrentLinkedDeque<PooledConnectionInternalInfo>> freeConnections;

    public PostgresInstancePooledConnectionsStorage() {
        allConnections = new ConcurrentHashMap<>();
        freeConnections = new ConcurrentHashMap<>();
    }

    /**
     * Adds new connection to storage and marks it as free.
     *
     * @param startupMessageInfo info about connection that was received from client in startup message
     * @param channel            representation of real connection to primary
     * @return a new pooled connection marked as 'free'
     */
    public PooledConnectionInternalInfo addNewChannel(StartupMessageInfo startupMessageInfo, Channel channel) {
        PooledConnectionInternalInfo pooledConnectionInternalInfo = PooledConnectionInternalInfo
                .builder()
                .startupMessageInfo(startupMessageInfo)
                .taken(new AtomicBoolean(false))
                .lastFreeTimestamp(System.currentTimeMillis())
                .realPostgresConnection(channel)
                .build();

        addConnectionToMap(startupMessageInfo, pooledConnectionInternalInfo, allConnections);
        addConnectionToMap(startupMessageInfo, pooledConnectionInternalInfo, freeConnections);

        return pooledConnectionInternalInfo;
    }

    /**
     * Adds new connection to storage and marks it as taken
     *
     * @param startupMessageInfo info about connection that was received from client in startup message
     * @param channel            representation of real connection to primary
     * @return a new pooled connection marked as 'taken'
     */
    public PooledConnectionInternalInfo addNewChannelAndMarkAsTaken(StartupMessageInfo startupMessageInfo, Channel channel) {
        PooledConnectionInternalInfo pooledConnectionInternalInfo = PooledConnectionInternalInfo
                .builder()
                .startupMessageInfo(startupMessageInfo)
                .taken(new AtomicBoolean(true))
                .lastFreeTimestamp(System.currentTimeMillis())
                .realPostgresConnection(channel)
                .build();

        addConnectionToMap(startupMessageInfo, pooledConnectionInternalInfo, allConnections);

        return pooledConnectionInternalInfo;
    }

    /**
     * Returns connection back to storage and marks it as 'free'. Such connection must have been added earlier and marked as 'taken'.
     * There are NO checks if connection was really added previously or marked as 'taken'! TChecks are not included to improve performance.
     *
     * @param pooledConnectionInternalInfo polled connection that was taken from the pool
     */
    public void returnTakenConnection(PooledConnectionInternalInfo pooledConnectionInternalInfo) {
        pooledConnectionInternalInfo.setLastFreeTimestamp(System.currentTimeMillis());
        pooledConnectionInternalInfo.getTaken().set(false);
        addConnectionToMap(pooledConnectionInternalInfo.getStartupMessageInfo(), pooledConnectionInternalInfo, freeConnections);
    }

    /**
     * Finds and returns free connection based on startup info. Returned connection will be marked as 'taken'.
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
        if (pooledConnectionInternalInfo.getTaken().compareAndSet(false, true)) {
            return pooledConnectionInternalInfo;
        }

        // corner case: taken connection was removed by cleaner
        return getFreeConnection(startupMessageInfo);
    }

    /**
     * Removes unneeded connections that was not in use for some period of time. Only this method can really remove connection from storage.
     * Only connections marked as 'free' are removed.
     * Concurrent calls of this method is forbidden!
     *
     * @param lastTimestamp identifying the oldest timestamp for connection which will not be removed
     * @return list of freed connections
     */
    public List<Channel> removeUnneededConnections(long lastTimestamp) {
        return freeConnections.values()
                .stream()
                .flatMap(Collection::stream)
                .filter(connection -> {
                    // check if connection is old enough to be removed.
                    // if old enough, then mark it as 'taken' to prevent it from being taken by client
                    if (connection.getLastFreeTimestamp() < lastTimestamp && connection.getTaken().compareAndSet(false, true)) {
                        removeConnection(connection);
                        return true;
                    }

                    return false;
                })
                .map(PooledConnectionInternalInfo::getRealPostgresConnection)
                .collect(Collectors.toList());
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

        freeConnections.get(startupMessageInfo).remove(connection);
        allConnections.get(startupMessageInfo).remove(connection);
    }
}
