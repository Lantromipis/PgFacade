package com.lantromipis.orchestration.orchestrator.impl;

import com.lantromipis.configuration.event.RaftLogSyncedOnStartupEvent;
import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.RaftException;
import com.lantromipis.orchestration.model.ExternalLoadBalancerAdapterInfo;
import com.lantromipis.orchestration.model.raft.ExternalLoadBalancerRaftInfo;
import com.lantromipis.orchestration.orchestrator.api.LoadBalancerOrchestrator;
import com.lantromipis.orchestration.restclient.ExternalLoadBalancerHealtcheckTemplateRestClient;
import com.lantromipis.orchestration.restclient.model.HealtcheckResponseDto;
import com.lantromipis.orchestration.util.DynamicRestClientUtils;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@ApplicationScoped
public class LoadBalancerOrchestratorImpl implements LoadBalancerOrchestrator {

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    DynamicRestClientUtils dynamicRestClientUtils;

    private AtomicBoolean orchestrationActive = new AtomicBoolean(false);
    private boolean raftSynced = false;
    private ExternalLoadBalancerHealtcheckTemplateRestClient cachedRestClient = null;
    private Instant timeWhenLoadBalancerExpectedToBeFullyStarted = null;
    private int failedHealthChecksCount = 0;

    private Object orchestrationLock = new Object[0];

    @Override
    @Synchronized("orchestrationLock")
    public void startOrchestration() {
        if (!orchestrationProperties.common().externalLoadBalancer().deploy()) {
            orchestrationActive.set(false);
            log.info("Will not start external load balancer orchestration because it is disabled by configuration!");
            return;
        }

        orchestrationActive.set(true);
        failedHealthChecksCount = 0;
        try {
            dynamicRestClientUtils.closeClient(cachedRestClient);
            cachedRestClient = null;

            startOrRedeployLoadBalancerAndCreateHealthcheckRestClient();
        } catch (Exception e) {
            log.error("Error while starting external load balancer orchestration!", e);
        }
    }

    @Override
    @Synchronized("orchestrationLock")
    public void stopOrchestration() {
        orchestrationActive.set(false);
        dynamicRestClientUtils.closeClient(cachedRestClient);
        cachedRestClient = null;
        timeWhenLoadBalancerExpectedToBeFullyStarted = null;
    }

    @Override
    public void shutdownLoadBalancer() {
        if (!PgFacadeRaftRole.LEADER.equals(pgFacadeRuntimeProperties.getRaftRole())) {
            return;
        }

        stopOrchestration();
        ExternalLoadBalancerRaftInfo raftInfo = raftFunctionalityCombinator.getPgFacadeLoadBalancerInfo();
        platformAdapter.get().stopExternalLoadBalancerInstance(raftInfo.getAdapterIdentifier());
    }

    public void syncedWithRaftLog(@Observes RaftLogSyncedOnStartupEvent raftLogSyncedOnStartupEvent) {
        raftSynced = true;
    }

    @Scheduled(every = "${pg-facade.orchestration.common.external-load-balancer.healthcheck-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void checkExternalLoadBalancerHealth() {
        if (!orchestrationActive.get()
                || !PgFacadeRaftRole.LEADER.equals(pgFacadeRuntimeProperties.getRaftRole())
                || !raftSynced
        ) {
            return;
        }


        try {
            // if not rest client, create it and possibly redeploy load balancer
            if (cachedRestClient == null) {
                startOrRedeployLoadBalancerAndCreateHealthcheckRestClient();
            }

            if (timeWhenLoadBalancerExpectedToBeFullyStarted != null && timeWhenLoadBalancerExpectedToBeFullyStarted.isBefore(Instant.now())) {
                // load balancer is starting up
                return;
            }
            // set null not to call isBefore()
            timeWhenLoadBalancerExpectedToBeFullyStarted = null;

            try {
                HealtcheckResponseDto healtcheckResponseDto = cachedRestClient.checkLiveliness();
                if (!HealtcheckResponseDto.HealtcheckStatus.UP.equals(healtcheckResponseDto.getStatus())) {
                    failedHealthChecksCount++;
                    log.error(
                            "External load balancer was unhealthy {} times! Will force redeploy it after {} attempts!",
                            failedHealthChecksCount,
                            orchestrationProperties.common().externalLoadBalancer().healthcheckRetries()
                    );

                }
            } catch (Exception e) {
                failedHealthChecksCount++;
                log.error(
                        "Healthcheck request to external load balancer failed {} times! Will force redeploy it after {} attempts!",
                        failedHealthChecksCount,
                        orchestrationProperties.common().externalLoadBalancer().healthcheckRetries()
                );
            }
        } catch (Exception e) {
            failedHealthChecksCount++;
            log.error(
                    "Healthcheck attempt for external load balancer failed {} times! Will force redeploy it after {} attempts!",
                    failedHealthChecksCount,
                    orchestrationProperties.common().externalLoadBalancer().healthcheckRetries()
            );
        }

        if (failedHealthChecksCount >= orchestrationProperties.common().externalLoadBalancer().healthcheckRetries()) {
            log.error("Reached maximum number of attempts to healthcheck external load balancer. Force redeploying it now!");
            deleteExistingLoadBalancerInstance();
            boolean newInstanceCreatedAndStarted = createAndStartNewLoadBalancerInstance();
            if (newInstanceCreatedAndStarted) {
                failedHealthChecksCount = 0;
            } else {
                log.info("Failed to start new external load balancer instance!");
            }
        }
    }

    private void startOrRedeployLoadBalancerAndCreateHealthcheckRestClient() {
        ExternalLoadBalancerRaftInfo raftInfo = raftFunctionalityCombinator.getPgFacadeLoadBalancerInfo();
        ExternalLoadBalancerAdapterInfo adapterInfo;

        if (raftInfo != null && StringUtils.isNotEmpty(raftInfo.getAdapterIdentifier())) {
            adapterInfo = platformAdapter.get().getExternalLoadBalancerInstanceInfo(raftInfo.getAdapterIdentifier());
            if (!adapterInfo.isRunning()) {
                adapterInfo = platformAdapter.get().startExternalLoadBalancerInstance(raftInfo.getAdapterIdentifier());
                timeWhenLoadBalancerExpectedToBeFullyStarted = Instant.now().plusMillis(orchestrationProperties.common().externalLoadBalancer().startupDuration().toMillis());
                log.info("Started existing external load balancer instance!");
            } else {
                log.info("External load balancer already running!");
            }

            recreateCachedHealthcheckRestClient(adapterInfo);
        } else {
            createAndStartNewLoadBalancerInstance();
        }
    }

    private void deleteExistingLoadBalancerInstance() {
        ExternalLoadBalancerRaftInfo raftInfo = raftFunctionalityCombinator.getPgFacadeLoadBalancerInfo();
        raftFunctionalityCombinator.savePgFacadeLoadBalancerInfo(ExternalLoadBalancerRaftInfo.builder().adapterIdentifier(null).build());

        platformAdapter.get().deleteInstance(raftInfo.getAdapterIdentifier());

        dynamicRestClientUtils.closeClient(cachedRestClient);
        cachedRestClient = null;
    }

    private boolean createAndStartNewLoadBalancerInstance() {
        String adapterIdentifier = platformAdapter.get().createExternalLoadBalancerInstance();
        ExternalLoadBalancerAdapterInfo adapterInfo = platformAdapter.get().startExternalLoadBalancerInstance(adapterIdentifier);
        timeWhenLoadBalancerExpectedToBeFullyStarted = Instant.now().plusMillis(orchestrationProperties.common().externalLoadBalancer().startupDuration().toMillis());
        log.info("Created and started new external load balancer instance!");

        ExternalLoadBalancerRaftInfo raftInfo = ExternalLoadBalancerRaftInfo
                .builder()
                .adapterIdentifier(adapterIdentifier)
                .createdWhen(Instant.now())
                .build();
        try {
            raftFunctionalityCombinator.savePgFacadeLoadBalancerInfo(raftInfo);
        } catch (RaftException e) {
            // failed to safe, means this is not leader
            platformAdapter.get().deleteInstance(adapterIdentifier);
            log.error("Failed to save external load balancer info in Raft! Is this PgFacade node a Raft leader?", e);
            return false;
        }

        recreateCachedHealthcheckRestClient(adapterInfo);
        return true;
    }

    private void recreateCachedHealthcheckRestClient(ExternalLoadBalancerAdapterInfo adapterInfo) {
        dynamicRestClientUtils.closeClient(cachedRestClient);
        cachedRestClient = dynamicRestClientUtils.createRestClient(
                ExternalLoadBalancerHealtcheckTemplateRestClient.class,
                adapterInfo.getAddress(),
                adapterInfo.getHttpPort(),
                orchestrationProperties.common().externalLoadBalancer().healthcheckTimeout().toMillis()
        );
    }
}
