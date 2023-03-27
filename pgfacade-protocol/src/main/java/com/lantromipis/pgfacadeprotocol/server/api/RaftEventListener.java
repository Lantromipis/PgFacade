package com.lantromipis.pgfacadeprotocol.server.api;

import com.lantromipis.pgfacadeprotocol.model.api.RaftRole;

public interface RaftEventListener {
    /**
     * Called only once by raft server when this raft node is synced with leader (commit indexes equals) or this node became leader.
     * First case: this node is alone in cluster, so it became leader. After that, Raft server will cal this method.
     * Second case: this node is NOT alone in cluster, and it synced own operation log AND state machine with leader.
     * <p>
     * This is first event to be called
     */
    void syncedWithLeaderOrSelfIsLeaderOnStartup();

    /**
     * Called by raft server when this raft node changed role
     *
     * @param newRaftRole new role in raft cluster: leader, follower, candidate
     */
    void selfRoleChanged(RaftRole newRaftRole);
}
