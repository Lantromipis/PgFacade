package com.lantromipis.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lantromipis.constant.BalancerConstants;
import com.lantromipis.health.ProxyHostsKnownLivelinessHealtcheck;
import com.lantromipis.model.BalancerHostInFileInfo;
import com.lantromipis.model.PgFacadeNodeInfo;
import com.lantromipis.properties.StartupProperties;
import com.lantromipis.restclient.client.PgFacadeTemplateStatsRestClient;
import com.lantromipis.restclient.model.PgFacadeHttpNodesInfoResponseDto;
import com.lantromipis.restclient.model.PgFacadeNodeHttpConnectionInfo;
import com.lantromipis.restclient.model.PgFacadeSelfInfoResponseDto;
import com.lantromipis.service.api.NodesInfoProvider;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import java.io.Closeable;
import java.io.File;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@ApplicationScoped
public class NodeInfoProviderImpl implements NodesInfoProvider {

    @Inject
    StartupProperties startupProperties;

    @Inject
    ManagedExecutor managedExecutor;

    @Inject
    @Liveness
    ProxyHostsKnownLivelinessHealtcheck proxyHostsKnownLivelinessHealtcheck;

    @Inject
    ObjectMapper objectMapper;

    private AtomicReference<Map<String, PgFacadeTemplateStatsRestClient>> clientsListReference;

    public static final TypeReference<List<BalancerHostInFileInfo>> HOSTS_IN_FILE_TYPE_REF = new TypeReference<>() {
    };

    @PostConstruct
    public void init() {
        PgFacadeTemplateStatsRestClient initialNodeRestClient = createPgFacadeHealtcheckRestClient(
                startupProperties.initialHttpHost(),
                startupProperties.initialHttpPort()
        );

        PgFacadeHttpNodesInfoResponseDto nodesInfos = initialNodeRestClient.getHttpNodesInfos();

        clientsListReference = new AtomicReference<>(nodesInfoToClients(nodesInfos.getHttpNodesInfo()));
        proxyHostsKnownLivelinessHealtcheck.setReady(true);

        closeClient(initialNodeRestClient);
    }


    @Override
    public void reloadHosts() {
        List<PgFacadeNodeHttpConnectionInfo> infos = getHostsFromFile();

        if (CollectionUtils.isNotEmpty(infos)) {
            Map<String, PgFacadeTemplateStatsRestClient> prevClients = clientsListReference.get();
            clientsListReference.set(nodesInfoToClients(infos));
            proxyHostsKnownLivelinessHealtcheck.setReady(true);
            prevClients.values().forEach(this::closeClient);

            log.info("Reloaded hosts from file.");
            return;
        }

        Map<String, PgFacadeNodeHttpConnectionInfo> addressToNodeInfoMap = new HashMap<>();

        for (var entry : clientsListReference.get().entrySet()) {
            try {
                PgFacadeHttpNodesInfoResponseDto nodesInfos = entry.getValue().getHttpNodesInfos();
                if (CollectionUtils.isEmpty(nodesInfos.getHttpNodesInfo())) {
                    continue;
                }

                nodesInfos.getHttpNodesInfo().forEach(info -> addressToNodeInfoMap.put(info.getAddress(), info));
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.error("Failed to retrieve nodes info from http host with address {}", entry.getKey(), e);
                } else {
                    log.error("Failed to retrieve nodes info from http host with address {}", entry.getKey());
                }
            }
        }

        if (addressToNodeInfoMap.isEmpty()) {
            // all known rest clients failed.
            proxyHostsKnownLivelinessHealtcheck.setReady(false);
            return;
        }

        Map<String, PgFacadeTemplateStatsRestClient> prevClients = clientsListReference.get();
        clientsListReference.set(nodesInfoToClients(new ArrayList<>(addressToNodeInfoMap.values())));
        proxyHostsKnownLivelinessHealtcheck.setReady(true);

        prevClients.values().forEach(this::closeClient);
    }

    @Override
    public List<PgFacadeNodeInfo> getNodeInfos() {
        List<CompletableFuture<PgFacadeNodeInfo>> completableFutures = new ArrayList<>();
        List<PgFacadeNodeInfo> ret = new ArrayList<>();

        for (var entry : clientsListReference.get().entrySet()) {
            completableFutures.add(
                    managedExecutor.supplyAsync(
                            () -> {
                                PgFacadeSelfInfoResponseDto selfInfo = entry.getValue().getSelfNodeInfo();

                                return PgFacadeNodeInfo
                                        .builder()
                                        .address(selfInfo.getExternalConnectionInfo().getAddress())
                                        .httpPort(selfInfo.getExternalConnectionInfo().getHttpPort())
                                        .primaryPort(selfInfo.getExternalConnectionInfo().getPrimaryPort())
                                        .standbyPort(selfInfo.getExternalConnectionInfo().getStandbyPort())
                                        .currentPrimaryConnections(selfInfo.getPoolInfo().getCurrentPrimaryPoolAllConnectionsCount() - selfInfo.getPoolInfo().getCurrentPrimaryPoolFreeConnectionsCount())
                                        .currentStandbyConnections(selfInfo.getPoolInfo().getCurrentStandbyPoolAllConnectionsCount() - selfInfo.getPoolInfo().getCurrentStandbyPoolFreeConnectionsCount())
                                        .maxPrimaryConnections(selfInfo.getPoolInfo().getPrimaryPoolConnectionLimit())
                                        .maxStandbyConnections(selfInfo.getPoolInfo().getStandbyPoolConnectionLimit())
                                        .build();
                            }
                    )
            );
        }

        for (var future : completableFutures) {
            try {
                ret.add(future.join());
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.error("Error while getting PgFacade nodes info. ", e);
                } else {
                    log.error("Error while getting PgFacade nodes info. ");
                }
            }
        }

        return ret;
    }

    private PgFacadeTemplateStatsRestClient createPgFacadeHealtcheckRestClient(String address, int port) {
        URI uri = URI.create("http://" + address + ":" + port);

        return RestClientBuilder.newBuilder()
                .baseUri(uri)
                .connectTimeout(100, TimeUnit.MILLISECONDS)
                .readTimeout(100, TimeUnit.MILLISECONDS)
                .build(PgFacadeTemplateStatsRestClient.class);
    }

    private Map<String, PgFacadeTemplateStatsRestClient> nodesInfoToClients(List<PgFacadeNodeHttpConnectionInfo> nodesInfo) {
        Map<String, PgFacadeTemplateStatsRestClient> ret = new HashMap<>();

        for (var info : nodesInfo) {
            ret.put(
                    info.getAddress(),
                    createPgFacadeHealtcheckRestClient(
                            info.getAddress(),
                            info.getHttpPort()
                    )
            );
        }

        return ret;
    }

    private void closeClient(Closeable client) {
        try {
            client.close();
        } catch (Exception ignored) {
        }
    }

    private List<PgFacadeNodeHttpConnectionInfo> getHostsFromFile() {
        File file = new File(BalancerConstants.BALANCER_HOSTS_FILE);

        try {
            if (!file.exists()) {
                return Collections.emptyList();
            }

            log.info("Found file with hosts. Will reload hosts using file.");

            List<BalancerHostInFileInfo> infos = objectMapper.readValue(file, HOSTS_IN_FILE_TYPE_REF);

            return infos
                    .stream()
                    .map(hostInfile -> PgFacadeNodeHttpConnectionInfo
                            .builder()
                            .address(hostInfile.getAddress())
                            .httpPort(hostInfile.getPort())
                            .build()
                    )
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Failed to reload hosts from file. File will be removed.", e);
            return Collections.emptyList();
        } finally {
            try {
                file.delete();
            } catch (Exception ignored) {
                //ignored
            }
        }
    }
}
