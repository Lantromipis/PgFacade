package com.lantromipis.orchestration.orchestrator.api;

public interface PgFacadeOrchestrator {
    void startOrchestration();

    void stopOrchestration();

    /**
     * If this node is Raft Leader, then shutdown each and every node in cluster.
     * If this node is NOT Raft Leader, then shutdown only self.
     *
     * @return true if shutdown was successful, false if some error occurred
     */
    boolean shutdownClusterFull(boolean force, boolean shutdownPostgres, boolean shutdownLoadBalancer, long maxProxyAwaitSeconds);

    /**
     * If this node is Raft Leader, then shutdown Raft Server and Orchestration on each and every node in cluster.
     * If this node is NOT Raft Leader, then shutdown only own Raft Server and Orchestration.
     */
    boolean shutdownClusterRaftAndOrchestration(boolean suspend);
}
