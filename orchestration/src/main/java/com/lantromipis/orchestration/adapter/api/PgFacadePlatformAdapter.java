package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.exception.PlatformAdapterOperationExecutionException;
import com.lantromipis.orchestration.model.PgFacadeNodeExternalConnectionsInfo;
import com.lantromipis.orchestration.model.PgFacadeNodeHttpConnectionsInfo;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;

import java.util.List;

public interface PgFacadePlatformAdapter {

    /**
     * Returns info about current PgFacade instance.
     *
     * @return info about current Pgfacade instance
     * @throws PlatformAdapterOperationExecutionException if something went wrong and operation failed
     */
    PgFacadeRaftNodeInfo getSelfRaftNodeInfo() throws PlatformAdapterOperationExecutionException;

    /**
     * Return list of active PgFacade instances. List must not contain inactive instances or instances that will be suspended.
     * Whether the instance is suspended or not is determined by the adapter. For example, for Docker suspended PgFacade containers will have some special prefix in name.
     *
     * @return list containing info about raft nodes
     * @throws PlatformAdapterOperationExecutionException if something went wrong and operation failed
     */
    List<PgFacadeRaftNodeInfo> getActiveRaftNodeInfos() throws PlatformAdapterOperationExecutionException;

    /**
     * Create and start new PgFacade instance.
     *
     * @return object containing raft node info for new instance
     * @throws PlatformAdapterOperationExecutionException if something went wrong and operation failed
     */
    PgFacadeRaftNodeInfo createAndStartNewPgFacadeInstance() throws PlatformAdapterOperationExecutionException;

    /**
     * Mark PgFacade instance as suspended. Suspended instances are not part of Raft group and are not managed by Raft leader.
     *
     * @param adapterIdentifier adapter identifier of PgFacade instance to be suspended
     * @throws PlatformAdapterOperationExecutionException if something went wrong and operation failed
     */
    void suspendPgFacadeInstance(String adapterIdentifier) throws PlatformAdapterOperationExecutionException;

    /**
     * Return HTTP info about active PgFacade instances. HTTP info contains address and port for REST server of PgFacade instance.
     *
     * @return collection of objects containing address and port of HTTP server for each PgFacade instance
     * @throws PlatformAdapterOperationExecutionException if something went wrong and operation failed
     */
    List<PgFacadeNodeHttpConnectionsInfo> getActivePgFacadeHttpNodesInfos() throws PlatformAdapterOperationExecutionException;

    /**
     * Return info about external address and ports which can be used by users to connect to PgFacade.
     *
     * @return object containing address and ports for external connections
     * @throws PlatformAdapterOperationExecutionException if something went wrong and operation failed
     */
    PgFacadeNodeExternalConnectionsInfo getSelfExternalConnectionInfo() throws PlatformAdapterOperationExecutionException;

    /**
     * Delete PgFacade instance
     *
     * @param adapterInstanceId adapter identifier of existing PgFacade instance
     * @return true if successfully deleted instance or if it is already deleted. False if failed to delete.
     */
    boolean deletePgFacadeInstance(String adapterInstanceId);
}
