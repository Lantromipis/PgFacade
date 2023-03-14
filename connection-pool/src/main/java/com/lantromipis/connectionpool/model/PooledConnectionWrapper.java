package com.lantromipis.connectionpool.model;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * POJO describing connection returned by connection pool
 */
@AllArgsConstructor
public class PooledConnectionWrapper {
    /**
     * Representation of real connection to Postgres instance
     */
    @Getter
    private Channel realPostgresConnection;

    /**
     * Contains messages describing server parameters. Should be sent in first message to client
     */
    @Getter
    private byte[] serverParameterMessagesBytes;

    /**
     * Runnable to called when it is time to return connection back to pool
     */
    private Runnable returnConnectionToPoolCallback;

    public void returnConnectionToPool() {
        returnConnectionToPoolCallback.run();
    }
}
