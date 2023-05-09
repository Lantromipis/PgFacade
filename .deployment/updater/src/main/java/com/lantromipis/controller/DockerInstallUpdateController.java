package com.lantromipis.controller;

import com.lantromipis.exception.InvalidParametersException;
import com.lantromipis.helper.DockerHelper;
import com.lantromipis.helper.PostgresConfigurationHelper;
import com.lantromipis.model.ConfigurationInfo;
import com.lantromipis.model.DockerInstallExistingRequestDto;
import com.lantromipis.model.DockerInstallNewRequestDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Slf4j
@Path("/docker")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DockerInstallUpdateController {

    @Inject
    DockerHelper dockerHelper;

    @Inject
    PostgresConfigurationHelper postgresConfigurationHelper;

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

        if (CollectionUtils.isEmpty(requestDto.getNetworksToConnectPgFacade())
                || requestDto.getNetworkBetweenPostgresAndPgFacade() == null
                || requestDto.getNetworksToConnectPgFacade().stream().noneMatch(network -> requestDto.getNetworkBetweenPostgresAndPgFacade().equals(network.getNetworkName()))) {
            throw new InvalidParametersException("Specify network between Postgres and PgFacade.");
        }

        String newPostgresContainerId = null;

        try {
            newPostgresContainerId = dockerHelper.createAndStartNewPostgres(
                    requestDto.getPostgresImageTag(),
                    requestDto.getNewSuperuserCredentials().getSuperuserName(),
                    requestDto.getNewSuperuserCredentials().getSuperuserPassword(),
                    requestDto.getNewSuperuserCredentials().getSuperuserDatabase(),
                    requestDto.getAwaitPgFacadeContainerMs()
            );

            String postgresAddress = dockerHelper.connectPostgresAndCurrentContainerTogether(newPostgresContainerId);

            postgresConfigurationHelper.configure(
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
                            .build()
            );

            String pgFacadeVolumeName = dockerHelper.createVolumeWithInitialPgFacadeData(newPostgresContainerId, null);
            String pgFacadeContainerId = dockerHelper.createAndStartNewPgFacadeContainer(
                    requestDto.getPgFacadeImageTag(),
                    pgFacadeVolumeName,
                    requestDto.getPgFacadeEnvVars(),
                    requestDto.isMountDockerSock() ? requestDto.getDockerSockPathOnHost() : null,
                    requestDto.getNetworksToConnectPgFacade(),
                    newPostgresContainerId,
                    requestDto.getNetworkBetweenPostgresAndPgFacade()
            );

            return Response
                    .ok()
                    .entity("Success! Your new PgFacade container id is " + pgFacadeContainerId)
                    .build();
        } finally {
            dockerHelper.cleanup(newPostgresContainerId);
        }
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

        if (CollectionUtils.isEmpty(requestDto.getNetworksToConnectPgFacade())
                || requestDto.getNetworkBetweenPostgresAndPgFacade() == null
                || requestDto.getNetworksToConnectPgFacade().stream().noneMatch(network -> requestDto.getNetworkBetweenPostgresAndPgFacade().equals(network.getNetworkName()))) {
            throw new InvalidParametersException("Specify network between Postgres and PgFacade.");
        }

        try {
            String postgresAddress = dockerHelper.connectPostgresAndCurrentContainerTogether(requestDto.getPostgresContainerId());

            if (requestDto.getConfigurationInfo().isConfigurePostgres()) {
                postgresConfigurationHelper.configure(
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
                                .build()
                );
            }

            String pgFacadeVolumeName = dockerHelper.createVolumeWithInitialPgFacadeData(requestDto.getPostgresContainerId(), requestDto.getModifiedPostgresConfParams());
            String pgFacadeContainerId = dockerHelper.createAndStartNewPgFacadeContainer(
                    requestDto.getPgFacadeImageTag(),
                    pgFacadeVolumeName,
                    requestDto.getPgFacadeEnvVars(),
                    requestDto.isMountDockerSock() ? requestDto.getDockerSockPathOnHost() : null,
                    requestDto.getNetworksToConnectPgFacade(),
                    requestDto.getPostgresContainerId(),
                    requestDto.getNetworkBetweenPostgresAndPgFacade()
            );

            return Response
                    .ok()
                    .entity("Success! Your new PgFacade container id is " + pgFacadeContainerId)
                    .build();
        } finally {
            dockerHelper.cleanup(requestDto.getPostgresContainerId());
        }
    }

    @POST
    @Path("/rolling-update")
    public void rollingUpdate() {

    }
}
