package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.exception.PlatformAdapterOperationExecutionException;
import com.lantromipis.orchestration.model.ExternalLoadBalancerAdapterInfo;

public interface LoadBalancerPlatformAdapter {
    /**
     * Create and start new external load balancer instance.
     *
     * @return adapter identifier
     * @throws PlatformAdapterOperationExecutionException if something went wrong and operation failed
     */
    String createExternalLoadBalancerInstance() throws PlatformAdapterOperationExecutionException;

    /**
     * Get adapter info about external load balancer
     *
     * @param adapterIdentifier adapter identifier of existing external load balancer instance
     * @return object containing adapter info about external load balancer
     * @throws PlatformAdapterOperationExecutionException if something went wrong and operation failed
     */
    ExternalLoadBalancerAdapterInfo getExternalLoadBalancerInstanceInfo(String adapterIdentifier) throws PlatformAdapterOperationExecutionException;

    /**
     * Start existing external load balancer instance.
     *
     * @return object containing info for created load balancer
     * @throws PlatformAdapterOperationExecutionException if something went wrong and operation failed
     */
    ExternalLoadBalancerAdapterInfo startExternalLoadBalancerInstance(String adapterIdentifier) throws PlatformAdapterOperationExecutionException;

    /**
     * Stop existing external load balancer instance.
     *
     * @param adapterIdentifier adapter identifier of existing external load balancer instance
     * @return true if successfully stopped load balancer or it is already stopped. False if failed to stop.
     */
    boolean stopExternalLoadBalancerInstance(String adapterIdentifier);

    /**
     * Delete external load balancer instance
     *
     * @param adapterInstanceId adapter identifier of existing external load balancer instance
     * @return true if successfully deleted instance or if it is already deleted. False if failed to delete.
     */
    boolean deleteExternalLoadBalancerInstance(String adapterInstanceId);
}
