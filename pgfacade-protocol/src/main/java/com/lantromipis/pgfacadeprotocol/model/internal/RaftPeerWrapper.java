package com.lantromipis.pgfacadeprotocol.model.internal;

import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;
import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;
import io.netty.channel.Channel;
import lombok.Data;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class RaftPeerWrapper {
    private RaftNode raftNode;
    private RaftRole role;
    private AtomicLong nextIndex;
    private AtomicLong matchIndex;
    private AtomicLong commitIndex;
    private Channel channelCreatedBySelf;
    private long lastTimeActive = 0;

    public RaftPeerWrapper(RaftNode raftNode) {
        this.raftNode = raftNode;
        role = RaftRole.FOLLOWER;
        nextIndex = new AtomicLong(0);
        matchIndex = new AtomicLong(-1);
        commitIndex = new AtomicLong(-1);
        lastTimeActive = System.currentTimeMillis();
    }

    public RaftPeerWrapper(RaftNode raftNode, long lastTimeActive) {
        this(raftNode);
        this.lastTimeActive = lastTimeActive;
    }

    public synchronized void setChannelCreatedBySelf(Channel channelCreatedBySelf) {
        if (this.channelCreatedBySelf != null) {
            this.channelCreatedBySelf.close();
        }
        this.channelCreatedBySelf = channelCreatedBySelf;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftPeerWrapper wrapper = (RaftPeerWrapper) o;

        return Objects.equals(raftNode.getId(), wrapper.raftNode.getId());
    }

    @Override
    public int hashCode() {
        return raftNode != null ? raftNode.hashCode() : 0;
    }
}
