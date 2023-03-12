package com.lantromipis.connectionpool.model;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

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
}
