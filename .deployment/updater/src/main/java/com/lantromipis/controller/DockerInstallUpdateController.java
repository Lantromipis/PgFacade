package com.lantromipis.controller;

import com.lantromipis.exception.InvalidParametersException;
import com.lantromipis.helper.BalancerHelper;
import com.lantromipis.helper.DockerHelper;
import com.lantromipis.helper.PgFacadeHelper;
import com.lantromipis.helper.PostgresConfigurationHelper;
import com.lantromipis.model.docker.ConfigurationInfo;
import com.lantromipis.model.docker.DockerInstallExistingRequestDto;
import com.lantromipis.model.docker.DockerInstallNewRequestDto;
import com.lantromipis.model.docker.DockerRollingUpdateRequestDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Path("/docker")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DockerInstallUpdateController {

    @Inject
    DockerHelper dockerHelper;

    @Inject
    PostgresConfigurationHelper postgresConfigurationHelper;

    @Inject
    PgFacadeHelper pgFacadeHelper;

    @Inject
    BalancerHelper balancerHelper;

    @POST
    @Path("/install-new-postgres")
    public Response installNew(DockerInstallNewRequestDto requestDto) throws Exception {

        if (StringUtils.isEmpty(requestDto.getPgFacadeImageTag())) {
            throw new InvalidParametersException("PgFacade image tag can not be empty");
        }

        if (StringUtils.isEmpty(requestDto.getPostgresImageTag())) {
            throw new InvalidParametersException("Postgres image tag can not be empty");
        }

        if (requestDto.getNewSuperuserCredentials() == null) {
            throw new InvalidParametersException("No credentials for new Postgres superuser provided");
        }

        if (StringUtils.isEmpty(requestDto.getPostgresConfigurationInfo().getPgFacadeUsername())) {
            throw new InvalidParametersException("Provide Pgfacade user username.");
        }
        if (StringUtils.isEmpty(requestDto.getPostgresConfigurationInfo().getPgFacadeDatabase())) {
            throw new InvalidParametersException("Provide Pgfacade user database.");
        }
        if (StringUtils.isEmpty(requestDto.getPostgresConfigurationInfo().getPgFacadePassword())) {
            throw new InvalidParametersException("Provide Pgfacade user password.");
        }
        if (requestDto.getPostgresConfigurationInfo().isCreateReplicationUser() && StringUtils.isEmpty(requestDto.getPostgresConfigurationInfo().getReplicationUsername())) {
            throw new InvalidParametersException("Provide replication user password.");
        }
        if (requestDto.getPostgresConfigurationInfo().isCreateReplicationUser() && StringUtils.isEmpty(requestDto.getPostgresConfigurationInfo().getReplicationPassword())) {
            throw new InvalidParametersException("Provide replication user password.");
        }

        if (requestDto.isMountDockerSock() && StringUtils.isEmpty(requestDto.getDockerSockPathOnHost())) {
            throw new InvalidParametersException("If you want to mount docker.sock, you need to provide path to it on host.");
        }

        if (requestDto.getNetworkBetweenPostgresAndPgFacade() == null || StringUtils.isEmpty(requestDto.getNetworkBetweenPostgresAndPgFacade().getNetworkName())) {
            throw new InvalidParametersException("Specify network between Postgres and PgFacade.");
        }

        if (requestDto.getInternalPgFacadeNetwork() == null || StringUtils.isEmpty(requestDto.getInternalPgFacadeNetwork().getNetworkName())) {
            throw new InvalidParametersException("Specify internal PgFacade network.");
        }

        if (requestDto.getExternalPgFacadeNetwork() == null || StringUtils.isEmpty(requestDto.getExternalPgFacadeNetwork().getNetworkName())) {
            throw new InvalidParametersException("Specify external PgFacade network.");
        }

        List<String> networks = new ArrayList<>();

        dockerHelper.createNetworkIfNeeded(requestDto.getNetworkBetweenPostgresAndPgFacade());
        dockerHelper.createNetworkIfNeeded(requestDto.getInternalPgFacadeNetwork());
        dockerHelper.createNetworkIfNeeded(requestDto.getExternalPgFacadeNetwork());

        networks.add(requestDto.getNetworkBetweenPostgresAndPgFacade().getNetworkName());
        networks.add(requestDto.getInternalPgFacadeNetwork().getNetworkName());
        networks.add(requestDto.getExternalPgFacadeNetwork().getNetworkName());

        if (requestDto.getLoadBalancerNetwork() != null && StringUtils.isNotEmpty(requestDto.getLoadBalancerNetwork().getNetworkName())) {
            dockerHelper.createNetworkIfNeeded(requestDto.getLoadBalancerNetwork());
            networks.add(requestDto.getLoadBalancerNetwork().getNetworkName());
        }

        if (CollectionUtils.isNotEmpty(requestDto.getOtherNetworksToConnectPgFacadeContainer())) {
            for (var net : requestDto.getOtherNetworksToConnectPgFacadeContainer()) {
                dockerHelper.createNetworkIfNeeded(net);
                networks.add(net.getNetworkName());
            }
        }

        String newPostgresContainerId = dockerHelper.createAndStartNewPostgres(
                requestDto.getPostgresImageTag(),
                requestDto.getNewSuperuserCredentials().getSuperuserName(),
                requestDto.getNewSuperuserCredentials().getSuperuserPassword(),
                requestDto.getNewSuperuserCredentials().getSuperuserDatabase(),
                requestDto.getAwaitPgFacadeContainerMs()
        );

        String postgresAddress = dockerHelper.connectOtherAndCurrentContainersTogether(newPostgresContainerId, requestDto.getNetworkBetweenPostgresAndPgFacade().getNetworkName());
        String postgresSubnet = dockerHelper.getSubnetOfNetwork(requestDto.getNetworkBetweenPostgresAndPgFacade().getNetworkName());

        postgresConfigurationHelper.configureDatabaseAndUser(
                postgresAddress,
                requestDto.getPostgresImagePort() == 0 ? 5432 : requestDto.getPostgresImagePort(),
                requestDto.getNewSuperuserCredentials().getSuperuserDatabase(),
                requestDto.getNewSuperuserCredentials().getSuperuserName(),
                requestDto.getNewSuperuserCredentials().getSuperuserPassword(),
                ConfigurationInfo
                        .builder()
                        .pgFacadeUsername(requestDto.getPostgresConfigurationInfo().getPgFacadeUsername())
                        .pgFacadePassword(requestDto.getPostgresConfigurationInfo().getPgFacadePassword())
                        .pgFacadeDatabase(requestDto.getPostgresConfigurationInfo().getPgFacadeDatabase())
                        .createReplicationUser(requestDto.getPostgresConfigurationInfo().isCreateReplicationUser())
                        .replicationUsername(requestDto.getPostgresConfigurationInfo().getReplicationUsername())
                        .replicationPassword(requestDto.getPostgresConfigurationInfo().getReplicationPassword())
                        .build(),
                postgresSubnet
        );

        String pgFacadeVolumeName = dockerHelper.createVolumeWithInitialPgFacadeData(newPostgresContainerId, null);
        String pgFacadeContainerId = dockerHelper.createAndStartNewPgFacadeContainer(
                requestDto.getPgFacadeImageTag(),
                pgFacadeVolumeName,
                requestDto.getPgFacadeEnvVars(),
                requestDto.isMountDockerSock() ? requestDto.getDockerSockPathOnHost() : null,
                networks
        );

        return Response
                .ok()
                .entity("Success! Your new PgFacade container id is " + pgFacadeContainerId)
                .build();
    }

    @POST
    @Path("/install-existing-postgres")
    public Response installExisting(DockerInstallExistingRequestDto requestDto) throws Exception {
        if (StringUtils.isEmpty(requestDto.getPgFacadeImageTag())) {
            throw new InvalidParametersException("PgFacade image tag can not be empty");
        }

        if (StringUtils.isEmpty(requestDto.getPostgresContainerId())) {
            throw new InvalidParametersException("Provide Postgres container id.");
        }

        if (requestDto.getConfigurationInfo() != null && requestDto.getConfigurationInfo().isConfigurePostgres()) {
            if (requestDto.getConfigurationInfo().getSuperuserCredentials() == null) {
                throw new InvalidParametersException("Provide superuser credentials.");
            }
            if (StringUtils.isEmpty(requestDto.getConfigurationInfo().getPgFacadeUsername())) {
                throw new InvalidParametersException("Provide Pgfacade user username.");
            }
            if (StringUtils.isEmpty(requestDto.getConfigurationInfo().getPgFacadeDatabase())) {
                throw new InvalidParametersException("Provide Pgfacade user database.");
            }
            if (StringUtils.isEmpty(requestDto.getConfigurationInfo().getPgFacadePassword())) {
                throw new InvalidParametersException("Provide Pgfacade user password.");
            }
            if (requestDto.getConfigurationInfo().isCreateReplicationUser() && StringUtils.isEmpty(requestDto.getConfigurationInfo().getReplicationUsername())) {
                throw new InvalidParametersException("Provide replication user password.");
            }
            if (requestDto.getConfigurationInfo().isCreateReplicationUser() && StringUtils.isEmpty(requestDto.getConfigurationInfo().getReplicationPassword())) {
                throw new InvalidParametersException("Provide replication user password.");
            }
        }

        if (requestDto.isMountDockerSock() && StringUtils.isEmpty(requestDto.getDockerSockPathOnHost())) {
            throw new InvalidParametersException("If you want to mount docker.sock, you need to provide path to it on host.");
        }

        if (requestDto.getNetworkBetweenPostgresAndPgFacade() == null || StringUtils.isEmpty(requestDto.getNetworkBetweenPostgresAndPgFacade().getNetworkName())) {
            throw new InvalidParametersException("Specify network between Postgres and PgFacade.");
        }

        if (requestDto.getInternalPgFacadeNetwork() == null || StringUtils.isEmpty(requestDto.getInternalPgFacadeNetwork().getNetworkName())) {
            throw new InvalidParametersException("Specify internal PgFacade network.");
        }

        if (requestDto.getExternalPgFacadeNetwork() == null || StringUtils.isEmpty(requestDto.getExternalPgFacadeNetwork().getNetworkName())) {
            throw new InvalidParametersException("Specify external PgFacade network.");
        }

        List<String> networks = new ArrayList<>();

        dockerHelper.createNetworkIfNeeded(requestDto.getNetworkBetweenPostgresAndPgFacade());
        dockerHelper.createNetworkIfNeeded(requestDto.getInternalPgFacadeNetwork());
        dockerHelper.createNetworkIfNeeded(requestDto.getExternalPgFacadeNetwork());

        networks.add(requestDto.getNetworkBetweenPostgresAndPgFacade().getNetworkName());
        networks.add(requestDto.getInternalPgFacadeNetwork().getNetworkName());
        networks.add(requestDto.getExternalPgFacadeNetwork().getNetworkName());

        if (requestDto.getLoadBalancerNetwork() != null && StringUtils.isNotEmpty(requestDto.getLoadBalancerNetwork().getNetworkName())) {
            dockerHelper.createNetworkIfNeeded(requestDto.getLoadBalancerNetwork());
            networks.add(requestDto.getLoadBalancerNetwork().getNetworkName());
        }

        if (CollectionUtils.isNotEmpty(requestDto.getOtherNetworksToConnectPgFacadeContainer())) {
            for (var net : requestDto.getOtherNetworksToConnectPgFacadeContainer()) {
                dockerHelper.createNetworkIfNeeded(net);
                networks.add(net.getNetworkName());
            }
        }

        String postgresAddress = dockerHelper.connectOtherAndCurrentContainersTogether(requestDto.getPostgresContainerId(), requestDto.getNetworkBetweenPostgresAndPgFacade().getNetworkName());
        String postgresSubnet = dockerHelper.getSubnetOfNetwork(requestDto.getNetworkBetweenPostgresAndPgFacade().getNetworkName());

        if (requestDto.getConfigurationInfo().isConfigurePostgres()) {
            postgresConfigurationHelper.configureDatabaseAndUser(
                    postgresAddress,
                    requestDto.getPostgresContainerPort() == 0 ? 5432 : requestDto.getPostgresContainerPort(),
                    requestDto.getConfigurationInfo().getSuperuserCredentials().getSuperuserDatabase(),
                    requestDto.getConfigurationInfo().getSuperuserCredentials().getSuperuserName(),
                    requestDto.getConfigurationInfo().getSuperuserCredentials().getSuperuserPassword(),
                    ConfigurationInfo
                            .builder()
                            .pgFacadeUsername(requestDto.getConfigurationInfo().getPgFacadeUsername())
                            .pgFacadePassword(requestDto.getConfigurationInfo().getPgFacadePassword())
                            .pgFacadeDatabase(requestDto.getConfigurationInfo().getPgFacadeDatabase())
                            .createReplicationUser(requestDto.getConfigurationInfo().isCreateReplicationUser())
                            .replicationUsername(requestDto.getConfigurationInfo().getReplicationUsername())
                            .replicationPassword(requestDto.getConfigurationInfo().getReplicationPassword())
                            .build(),
                    postgresSubnet
            );
        }

        String pgFacadeVolumeName = dockerHelper.createVolumeWithInitialPgFacadeData(requestDto.getPostgresContainerId(), requestDto.getModifiedPostgresConfParams());
        String pgFacadeContainerId = dockerHelper.createAndStartNewPgFacadeContainer(
                requestDto.getPgFacadeImageTag(),
                pgFacadeVolumeName,
                requestDto.getPgFacadeEnvVars(),
                requestDto.isMountDockerSock() ? requestDto.getDockerSockPathOnHost() : null,
                networks
        );

        return Response
                .ok()
                .entity("Success! Your new PgFacade container id is " + pgFacadeContainerId)
                .build();

    }

    @POST
    @Path("/rolling-update")
    public Response rollingUpdate(DockerRollingUpdateRequestDto requestDto) throws Exception {
        if (StringUtils.isEmpty(requestDto.getLeaderContainerId())) {
            throw new InvalidParametersException("Specify PgFacade leader container id.");
        }

        if (StringUtils.isEmpty(requestDto.getLoadBalancerContainerId())) {
            throw new InvalidParametersException("Specify load balancer container id.");
        }

        if (StringUtils.isEmpty(requestDto.getNewPgFacadeImageTag())) {
            throw new InvalidParametersException("Specify new PgFacade image tag.");
        }

        if (requestDto.getLoadBalancerRefreshIntervalSeconds() == 0) {
            throw new InvalidParametersException("Specify load balancer config refresh interval.");
        }

        if (requestDto.getOldNodesAwaitClientsSeconds() == 0) {
            throw new InvalidParametersException("Specify await clients interval.");
        }

        if (requestDto.getPgFacadeHttpPort() == 0) {
            throw new InvalidParametersException("Specify PgFacade HTTP port.");
        }

        if (StringUtils.isEmpty(requestDto.getPgFacadeInternalNetworkName())) {
            throw new InvalidParametersException("Specify PgFacade internal network name.");
        }

        if (StringUtils.isEmpty(requestDto.getPgFacadeExternalNetworkName())) {
            throw new InvalidParametersException("Specify PgFacade external network name.");
        }

        if (MapUtils.isEmpty(requestDto.getNewPgFacadeEnvVars())) {
            throw new InvalidParametersException("Specify new env vars or provide {}.");
        }

        if (requestDto.isMountDockerSock() && StringUtils.isEmpty(requestDto.getDockerSockPathOnHost())) {
            throw new InvalidParametersException("If you want to mount docker.sock, you need to provide path to it on host.");
        }

        if (CollectionUtils.isEmpty(requestDto.getNetworkNamesToConnect())) {
            throw new InvalidParametersException("Specify networks to connect PgFacade container to.");
        }

        String pgFacadeContainerIp = dockerHelper.connectOtherAndCurrentContainersTogether(requestDto.getLeaderContainerId(), requestDto.getPgFacadeInternalNetworkName());

        pgFacadeHelper.shutdownRaftAndOrchestration(pgFacadeContainerIp, requestDto.getPgFacadeHttpPort());

        String newVolumeName = dockerHelper.createVolumeBasedOnOldPgFacade(requestDto.getLeaderContainerId());
        String newPgFacadeContainerId = dockerHelper.createAndStartNewPgFacadeContainer(
                requestDto.getNewPgFacadeImageTag(),
                newVolumeName,
                requestDto.getNewPgFacadeEnvVars(),
                requestDto.isMountDockerSock() ? requestDto.getDockerSockPathOnHost() : null,
                requestDto.getNetworkNamesToConnect()
        );

        // TODO use polling healthcheck
        Thread.sleep(30000);

        String pgFacadeLeaderIp = dockerHelper.getContainerIpInNetwork(newPgFacadeContainerId, requestDto.getPgFacadeExternalNetworkName());
        String loadBalancerHostsFileContent = balancerHelper.hostFileContentForSingleHost(pgFacadeLeaderIp, requestDto.getPgFacadeHttpPort());
        dockerHelper.executeCmdInContainer(
                requestDto.getLoadBalancerContainerId(),
                "echo '" + loadBalancerHostsFileContent + "' > " + balancerHelper.getHostFilePath()
        );

        // waiting for config to apply
        Thread.sleep((requestDto.getLoadBalancerRefreshIntervalSeconds() + 1) * 1000L);

        pgFacadeHelper.shutdownSoft(pgFacadeContainerIp, requestDto.getPgFacadeHttpPort(), requestDto.getOldNodesAwaitClientsSeconds());

        return Response
                .ok()
                .entity("Success! Your new PgFacade container id is " + newPgFacadeContainerId)
                .build();
    }
}
