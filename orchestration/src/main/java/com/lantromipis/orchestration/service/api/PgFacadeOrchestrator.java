package com.lantromipis.orchestration.service.api;

public interface PgFacadeOrchestrator {
    void startOrchestration();

    void stopOrchestration();

    /**
     * If this node is Raft Leader, then shutdown Raft Server on each and every node in cluster.
     * If this node is NOT Raft Leader, then shutdown only own Raft Server.
     *
     * @return true if shutdown was successful, false if some error occurred
     */
    boolean shutdownRaftForce();
}
