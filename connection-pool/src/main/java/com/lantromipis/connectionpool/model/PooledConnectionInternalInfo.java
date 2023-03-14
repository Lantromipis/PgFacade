package com.lantromipis.connectionpool.model;

import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * POJO containing internal info about pooled connection. Must be used only by connection pool and not by consumers!
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PooledConnectionInternalInfo {
    private StartupMessageInfo startupMessageInfo;
    private Channel realPostgresConnection;
    private AtomicBoolean taken;
    private long lastFreeTimestamp;
    private long createdTimestamp;
    private byte[] serverParameters;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PooledConnectionInternalInfo that = (PooledConnectionInternalInfo) o;

        if (lastFreeTimestamp != that.lastFreeTimestamp) {
            return false;
        }
        if (createdTimestamp != that.createdTimestamp) {
            return false;
        }
        if (!Objects.equals(realPostgresConnection, that.realPostgresConnection)) {
            return false;
        }
        return Objects.equals(taken, that.taken);
    }

    @Override
    public int hashCode() {
        int result = realPostgresConnection != null ? realPostgresConnection.hashCode() : 0;
        result = 31 * result + (taken != null ? taken.hashCode() : 0);
        result = 31 * result + (int) (lastFreeTimestamp ^ (lastFreeTimestamp >>> 32));
        result = 31 * result + (int) (createdTimestamp ^ (createdTimestamp >>> 32));
        return result;
    }
}
