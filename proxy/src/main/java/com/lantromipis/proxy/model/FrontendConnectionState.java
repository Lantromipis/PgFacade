package com.lantromipis.proxy.model;

import com.lantromipis.connectionpool.model.PooledConnectionWrapper;
import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FrontendConnectionState {
    boolean loadBalancing;
    boolean singleMaster;

    boolean inTransaction;
    boolean firstWriteStatementInTransactionIssued;
    boolean serializableTransaction;

    private AtomicBoolean resourcesFreed;

    private PooledConnectionWrapper primaryConnectionWrapper;
    private PooledConnectionWrapper standbyConnectionWrapper;

    private Channel primaryChannel;
    private Channel standbyChannel;

    private ByteBuf prevMessageLeftovers;
    private List<MessageInfo> messageInfos;

    public void replacePevMessageLeftovers(ByteBuf newLeftovers) {
        if (prevMessageLeftovers != null) {
            prevMessageLeftovers.release();
        }
        prevMessageLeftovers = newLeftovers;
    }
}
