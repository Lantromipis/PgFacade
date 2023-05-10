package com.lantromipis.helper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Network;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.lantromipis.model.DockerNetworkDto;
import com.lantromipis.model.copy.PostgresPersistedInstanceInfoCopy;
import com.lantromipis.properties.UpdaterProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@ApplicationScoped
public class DockerHelper {

    @Inject
    UpdaterProperties updaterProperties;

    @Inject
    ObjectMapper objectMapper;

    private DockerClient dockerClient;

    private static final String DOCKER_ENV_VAR_HOSTNAME = "HOSTNAME";
    private static final String POSTGRES_ENV_VAR_USERNAME = "POSTGRES_USER";
    private static final String POSTGRES_ENV_VAR_PASSWORD = "POSTGRES_PASSWORD";
    private static final String POSTGRES_ENV_VAR_DB = "POSTGRES_DB";

    private static final String PG_FACADE_VOLUME_MOUNT_PATH = "/var/run/pgfacade";
    private static final String PG_FACADE_STORED_FILES_DIR = "/var/run/pgfacade/stored";
    private static final String PG_FACADE_POSTGRES_NODES_INFO_FILE_NAME = "postgres-nodes-info.json";
    private static final String PG_FACADE_POSTGRES_SETTINGS_INFO_FILE_NAME = "postgres-settings-info.json";
    private static final String PG_FACADE_DOCKER_SOCK_PATH = "/var/run/pgfacade/docker.sock";
    private static final String PG_FACADE_DISCOVERY_LABEL = "pg-facade-discovery-label";

    private void initIfNeeded() {
        if (dockerClient != null) {
            return;
        }

        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost(updaterProperties.docker().host())
                .build();

        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .build();

        dockerClient = DockerClientImpl.getInstance(config, httpClient);
    }

    public void createNetworkIfNeeded(DockerNetworkDto dockerNetworkDto) {
        initIfNeeded();

        List<Network> networks = dockerClient
                .listNetworksCmd()
                .withNameFilter(dockerNetworkDto.getNetworkName())
                .exec();

        if (!dockerNetworkDto.isCreate() && CollectionUtils.isEmpty(networks)) {
            throw new RuntimeException("Can not find network with name " + dockerNetworkDto.getNetworkName() + "! Please check network name or set create flag to true.");
        }

        if (dockerNetworkDto.isCreate()) {
            if (CollectionUtils.isNotEmpty(networks)) {
                throw new RuntimeException("Network with name " + dockerNetworkDto.getNetworkName() + " already exists! Choose another network name or do not ask to creat it.");
            }

            dockerClient.createNetworkCmd()
                    .withName(dockerNetworkDto.getNetworkName())
                    .exec();
        }
    }

    public String createAndStartNewPostgres(String imageTag, String superName, String superPass, String superDb, int awaitMs) throws InterruptedException {
        initIfNeeded();
        CreateContainerResponse createContainerResponse = dockerClient.createContainerCmd(imageTag)
                .withName("pg-facade-managed-postgres-" + UUID.randomUUID().toString())
                .withHostConfig(
                        HostConfig.newHostConfig()
                )
                .withEnv(
                        List.of(
                                createEnvValueForRequest(POSTGRES_ENV_VAR_PASSWORD, superName),
                                createEnvValueForRequest(POSTGRES_ENV_VAR_USERNAME, superPass),
                                createEnvValueForRequest(POSTGRES_ENV_VAR_DB, superDb)
                        )
                )
                .exec();

        String containerId = createContainerResponse.getId();
        dockerClient.startContainerCmd(containerId).exec();

        Thread.sleep(awaitMs == 0 ? 15000 : awaitMs);

        return containerId;
    }

    public String connectPostgresAndCurrentContainerTogether(String postgresContainerId, String pgNetwork) {
        initIfNeeded();

        InspectContainerResponse inspectSelfResponse = inspectSelf();

        dockerClient.connectToNetworkCmd()
                .withContainerId(inspectSelfResponse.getId())
                .withNetworkId(pgNetwork)
                .exec();

        dockerClient.connectToNetworkCmd()
                .withContainerId(postgresContainerId)
                .withNetworkId(pgNetwork)
                .exec();

        InspectContainerResponse inspectPostgresResponse = dockerClient
                .inspectContainerCmd(postgresContainerId)
                .exec();

        return inspectPostgresResponse.getNetworkSettings()
                .getNetworks()
                .get(pgNetwork)
                .getIpAddress();
    }

    public String getSubnetOfNetwork(String networkName) {
        try {
            Network network = dockerClient.inspectNetworkCmd()
                    .withNetworkId(networkName)
                    .exec();

            return network.getIpam().getConfig().get(0).getSubnet();
        } catch (Exception e) {
            log.error("Error getting subnet for network with name " + networkName);
            return null;
        }
    }

    public String createVolumeWithInitialPgFacadeData(String postgresContainerId, Map<String, String> modifiedPgSettings) {
        initIfNeeded();

        CreateVolumeResponse createVolumeResponse = dockerClient
                .createVolumeCmd()
                .withName("pg-facade-initial-volume-" + UUID.randomUUID().toString())
                .exec();

        InspectContainerResponse inspectSelfResponse = inspectSelf();

        // create dummy container to get volume
        CreateContainerResponse createContainerResponse = dockerClient
                .createContainerCmd(inspectSelfResponse.getImageId())
                .withHostConfig(HostConfig.newHostConfig()
                        .withBinds(
                                new Bind(
                                        createVolumeResponse.getName(),
                                        new Volume(PG_FACADE_VOLUME_MOUNT_PATH)
                                )
                        )
                )
                .withEntrypoint("sleep", "infinity")
                .exec();

        dockerClient.startContainerCmd(createContainerResponse.getId()).exec();

        executeCmdInContainer(
                createContainerResponse.getId(),
                "mkdir " + PG_FACADE_STORED_FILES_DIR
        );

        UUID uuid = UUID.randomUUID();

        PostgresPersistedInstanceInfoCopy persistedInstanceInfoCopy =
                PostgresPersistedInstanceInfoCopy
                        .builder()
                        .adapterIdentifier(postgresContainerId)
                        .instanceId(uuid)
                        .primary(true)
                        .build();

        Map<UUID, PostgresPersistedInstanceInfoCopy> mapToSave = new HashMap<>();
        mapToSave.put(uuid, persistedInstanceInfoCopy);

        String postgresNodesFileContent;
        try {
            postgresNodesFileContent = objectMapper.writeValueAsString(mapToSave);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        executeCmdInContainer(
                createContainerResponse.getId(),
                "echo '" + postgresNodesFileContent + "' > " + PG_FACADE_STORED_FILES_DIR + "/" + PG_FACADE_POSTGRES_NODES_INFO_FILE_NAME
        );

        if (MapUtils.isNotEmpty(modifiedPgSettings)) {
            String settingString;
            try {
                settingString = objectMapper.writeValueAsString(modifiedPgSettings);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            executeCmdInContainer(
                    createContainerResponse.getId(),
                    "echo '" + settingString + "' > " + PG_FACADE_STORED_FILES_DIR + "/" + PG_FACADE_POSTGRES_SETTINGS_INFO_FILE_NAME
            );
        }

        dockerClient.stopContainerCmd(createContainerResponse.getId())
                .exec();

        dockerClient.removeContainerCmd(createContainerResponse.getId())
                .withForce(true)
                .exec();

        return createVolumeResponse.getName();
    }

    private InspectContainerResponse inspectSelf() {
        String hostname = System.getenv(DOCKER_ENV_VAR_HOSTNAME);

        if (hostname == null) {
            throw new RuntimeException("Docker error. No HOSTNAME env var in container.");
        }

        return dockerClient
                .inspectContainerCmd(hostname)
                .exec();
    }

    public String createAndStartNewPgFacadeContainer(String imageTag,
                                                     String volumeName,
                                                     Map<String, String> envVars,
                                                     String dockerSocketOnHostPath,
                                                     List<String> networksToConnect,
                                                     String postgresContainerId,
                                                     String pgNetwork) {
        initIfNeeded();
        List<Bind> binds = new ArrayList<>();

        binds.add(
                new Bind(
                        volumeName,
                        new Volume(PG_FACADE_VOLUME_MOUNT_PATH)
                )
        );

        if (dockerSocketOnHostPath != null) {
            binds.add(
                    new Bind(
                            dockerSocketOnHostPath,
                            new Volume(PG_FACADE_DOCKER_SOCK_PATH)
                    )
            );
        }

        CreateContainerCmd createContainerCmd = dockerClient.createContainerCmd(imageTag)
                .withName("pg-facade-node-" + UUID.randomUUID().toString())
                .withHostConfig(
                        HostConfig.newHostConfig()
                                .withBinds(binds)
                )
                .withLabels(Map.of(PG_FACADE_DISCOVERY_LABEL, "true"));

        if (MapUtils.isNotEmpty(envVars)) {
            createContainerCmd.withEnv(
                    envVars.entrySet()
                            .stream()
                            .map(entry -> createEnvValueForRequest(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList())
            );
        }

        CreateContainerResponse createContainerResponse = createContainerCmd.exec();

        for (var network : networksToConnect) {
            dockerClient.connectToNetworkCmd()
                    .withContainerId(createContainerResponse.getId())
                    .withNetworkId(network)
                    .exec();
        }

        dockerClient.startContainerCmd(createContainerResponse.getId()).exec();

        return createContainerResponse.getId();
    }

    private String createEnvValueForRequest(String varName, String value) {
        return varName + "=" + value;
    }

    private void executeCmdInContainer(String containerId, String shellCommand) {
        ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
        ByteArrayOutputStream stdErr = new ByteArrayOutputStream();

        try {
            ExecCreateCmd backupExecCreateCmd = dockerClient.execCreateCmd(containerId)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .withCmd("/bin/sh", "-c", shellCommand);

            ExecCreateCmdResponse backupExecCreateResponse = backupExecCreateCmd.exec();

            dockerClient.execStartCmd(backupExecCreateResponse.getId())
                    .withDetach(false)
                    .exec(new ExecStartResultCallback(stdOut, stdErr))
                    .awaitCompletion();

            InspectExecResponse inspectBackupExecResponse = dockerClient.inspectExecCmd(backupExecCreateResponse.getId()).exec();

            if (inspectBackupExecResponse.getExitCodeLong() != 0) {
                throw new RuntimeException("Failed to save PgFacade settings." + stdErr.toString());
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stdErr.close();
                stdOut.close();
            } catch (Exception ignored) {
                //ignored
            }
        }
    }
}
