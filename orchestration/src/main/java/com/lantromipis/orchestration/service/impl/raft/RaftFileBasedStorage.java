package com.lantromipis.orchestration.service.impl.raft;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lantromipis.configuration.exception.ConfigurationInitializationException;
import com.lantromipis.configuration.exception.PropertyModificationException;
import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.configuration.producers.FilesPathsProducer;
import com.lantromipis.orchestration.model.raft.PgFacadeLoadBalancerInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedArchiveInfo;
import com.lantromipis.orchestration.model.raft.PostgresPersistedInstanceInfo;
import com.lantromipis.orchestration.service.api.raft.RaftStorage;
import com.lantromipis.pgfacadeprotocol.model.api.SnapshotChunk;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static com.lantromipis.orchestration.constant.RaftConstants.*;

@Slf4j
@ApplicationScoped
public class RaftFileBasedStorage implements RaftStorage {

    @Inject
    FilesPathsProducer filesPathsProducer;

    private ObjectMapper objectMapper;

    private File postgresNodeInfoFile;
    private File postgresSettingInfoFile;
    private File postgresArchiveInfoFile;
    private File pgfacadeLoadBalancerInfoFile;
    private final ReentrantLock postgresNodeInfoFileModificationLock = new ReentrantLock();
    private final ReentrantLock postgresSettingInfoFileModificationLock = new ReentrantLock();
    private final ReentrantLock postgresArchiveInfoFileModificationLock = new ReentrantLock();
    private final ReentrantLock pgfacadeLoadBalancerInfoLock = new ReentrantLock();

    // @formatter:off
    public static final TypeReference<Map<UUID, PostgresPersistedInstanceInfo>> POSTGRES_NODE_INFO_TYPE_REF = new TypeReference<>() {};
    public static final TypeReference<Map<String, String>> POSTGRES_SETTING_INFO_TYPE_REF = new TypeReference<>() {};
    // @formatter:on

    @PostConstruct
    public void init() {
        objectMapper = new ObjectMapper();

        postgresNodeInfoFile = createConfigFileIfNeeded(filesPathsProducer.getPostgresNodesInfosFilePath());
        postgresSettingInfoFile = createConfigFileIfNeeded(filesPathsProducer.getPostgresSettingsInfosFilePath());
        postgresArchiveInfoFile = createConfigFileIfNeeded(filesPathsProducer.getPostgresArchiveInfosFilePath());
        pgfacadeLoadBalancerInfoFile = createConfigFileIfNeeded(filesPathsProducer.getPgFacadeLoadBalancerInfoFilePath());
    }

    @Override
    public List<SnapshotChunk> getChunks() {
        try {
            postgresNodeInfoFileModificationLock.lock();
            postgresSettingInfoFileModificationLock.lock();
            postgresArchiveInfoFileModificationLock.lock();
            pgfacadeLoadBalancerInfoLock.lock();

            List<SnapshotChunk> ret = new ArrayList<>();

            ret.add(SnapshotChunk
                    .builder()
                    .data(Files.readAllBytes(postgresNodeInfoFile.toPath()))
                    .name(POSTGRES_NODES_INFO_CHUNK)
                    .build()
            );
            ret.add(SnapshotChunk
                    .builder()
                    .data(Files.readAllBytes(postgresSettingInfoFile.toPath()))
                    .name(POSTGRES_SETTINGS_INFO_CHUNK)
                    .build()
            );
            ret.add(SnapshotChunk
                    .builder()
                    .data(Files.readAllBytes(postgresArchiveInfoFile.toPath()))
                    .name(POSTGRES_ARCHIVE_INFO_CHUNK)
                    .build()
            );
            ret.add(SnapshotChunk
                    .builder()
                    .data(Files.readAllBytes(pgfacadeLoadBalancerInfoFile.toPath()))
                    .name(PGFACADE_LOAD_BALANCER_ARCHIVE_INFO_CHUNK)
                    .build()
            );

            return ret;

        } catch (Exception e) {
            throw new PropertyReadException("Error while reading nodes info from file", e);
        } finally {
            postgresNodeInfoFileModificationLock.unlock();
            postgresSettingInfoFileModificationLock.unlock();
            postgresArchiveInfoFileModificationLock.unlock();
            pgfacadeLoadBalancerInfoLock.unlock();
        }
    }

    @Override
    public void loadChunks(List<SnapshotChunk> chunks) throws PropertyModificationException {
        try {
            postgresNodeInfoFileModificationLock.lock();
            postgresSettingInfoFileModificationLock.lock();
            postgresArchiveInfoFileModificationLock.lock();
            pgfacadeLoadBalancerInfoLock.lock();

            for (var chunk : chunks) {
                switch (chunk.getName()) {
                    case POSTGRES_NODES_INFO_CHUNK ->
                            FileUtils.writeByteArrayToFile(postgresNodeInfoFile, chunk.getData(), false);
                    case POSTGRES_SETTINGS_INFO_CHUNK ->
                            FileUtils.writeByteArrayToFile(postgresSettingInfoFile, chunk.getData(), false);
                    case POSTGRES_ARCHIVE_INFO_CHUNK ->
                            FileUtils.writeByteArrayToFile(postgresArchiveInfoFile, chunk.getData(), false);
                    case PGFACADE_LOAD_BALANCER_ARCHIVE_INFO_CHUNK ->
                            FileUtils.writeByteArrayToFile(pgfacadeLoadBalancerInfoFile, chunk.getData(), false);
                }
            }

        } catch (Exception e) {
            throw new PropertyModificationException("Error while reading nodes info from file", e);
        } finally {
            postgresNodeInfoFileModificationLock.unlock();
            postgresSettingInfoFileModificationLock.unlock();
            postgresArchiveInfoFileModificationLock.unlock();
            pgfacadeLoadBalancerInfoLock.unlock();
        }
    }

    @Override
    public PostgresPersistedArchiveInfo getArchiveInfo() throws PropertyReadException {
        try {
            postgresArchiveInfoFileModificationLock.lock();
            if (postgresArchiveInfoFile.length() > 0) {
                return objectMapper.readValue(postgresArchiveInfoFile, PostgresPersistedArchiveInfo.class);
            } else {
                return new PostgresPersistedArchiveInfo();
            }
        } catch (Exception e) {
            throw new PropertyReadException("Error while reading archive info from file", e);
        } finally {
            postgresArchiveInfoFileModificationLock.unlock();
        }
    }

    @Override
    public void saveArchiveInfo(PostgresPersistedArchiveInfo info) throws PropertyModificationException {
        try {
            postgresArchiveInfoFileModificationLock.lock();
            objectMapper.writeValue(postgresArchiveInfoFile, info);
        } catch (Exception e) {
            throw new PropertyModificationException("Error while saving archive info to file", e);
        } finally {
            postgresArchiveInfoFileModificationLock.unlock();
        }
    }

    @Override
    public List<PostgresPersistedInstanceInfo> getPostgresNodeInfos() throws PropertyReadException {
        try {
            postgresNodeInfoFileModificationLock.lock();
            if (postgresNodeInfoFile.length() > 0) {
                return new ArrayList<>(objectMapper.readValue(postgresNodeInfoFile, POSTGRES_NODE_INFO_TYPE_REF).values());
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            throw new PropertyReadException("Error while reading nodes info from file", e);
        } finally {
            postgresNodeInfoFileModificationLock.unlock();
        }
    }

    @Override
    public PostgresPersistedInstanceInfo getPostgresNodeInfo(UUID instanceId) throws PropertyReadException {
        return getPostgresNodeInfos()
                .stream()
                .filter(info -> Objects.equals(instanceId, info.getInstanceId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public void savePostgresNodeInfo(PostgresPersistedInstanceInfo postgresPersistedInstanceInfo) throws PropertyModificationException {
        try {
            postgresNodeInfoFileModificationLock.lock();
            Map<UUID, PostgresPersistedInstanceInfo> savedMap;

            if (postgresNodeInfoFile.length() > 0) {
                savedMap = objectMapper.readValue(postgresNodeInfoFile, POSTGRES_NODE_INFO_TYPE_REF);
            } else {
                savedMap = new HashMap<>();
            }

            savedMap.put(postgresPersistedInstanceInfo.getInstanceId(), postgresPersistedInstanceInfo);
            objectMapper.writeValue(postgresNodeInfoFile, savedMap);
        } catch (Exception e) {
            throw new PropertyModificationException("Error while saving nodes info to file", e);
        } finally {
            postgresNodeInfoFileModificationLock.unlock();
        }
    }

    @Override
    public PostgresPersistedInstanceInfo deletePostgresNodeInfo(UUID instanceId) throws PropertyModificationException {
        try {
            postgresNodeInfoFileModificationLock.lock();
            Map<UUID, PostgresPersistedInstanceInfo> savedMap;

            if (postgresNodeInfoFile.length() > 0) {
                savedMap = objectMapper.readValue(postgresNodeInfoFile, POSTGRES_NODE_INFO_TYPE_REF);
            } else {
                savedMap = new HashMap<>();
            }

            PostgresPersistedInstanceInfo ret = savedMap.remove(instanceId);
            if (ret != null) {
                objectMapper.writeValue(postgresNodeInfoFile, savedMap);
            }

            return ret;
        } catch (Exception e) {
            throw new PropertyModificationException("Error while saving nodes info to file", e);
        } finally {
            postgresNodeInfoFileModificationLock.unlock();
        }
    }

    @Override
    public void clearPostgresNodesInfos() throws PropertyModificationException {
        try {
            postgresNodeInfoFileModificationLock.lock();
            objectMapper.writeValue(postgresNodeInfoFile, new HashMap<>());
        } catch (Exception e) {
            throw new PropertyModificationException("Error while saving nodes info to file", e);
        } finally {
            postgresNodeInfoFileModificationLock.unlock();
        }
    }

    @Override
    public Map<String, String> getPostgresSettingInfos() throws PropertyReadException {
        try {
            postgresSettingInfoFileModificationLock.lock();
            if (postgresSettingInfoFile.length() > 0) {
                return objectMapper.readValue(postgresSettingInfoFile, POSTGRES_SETTING_INFO_TYPE_REF);
            } else {
                return Collections.emptyMap();
            }
        } catch (Exception e) {
            throw new PropertyReadException("Error while reading settings info from file", e);
        } finally {
            postgresSettingInfoFileModificationLock.unlock();
        }
    }

    @Override
    public void savePostgresSettingsInfos(Map<String, String> settingsToSave) throws PropertyModificationException {
        try {
            postgresSettingInfoFileModificationLock.lock();
            Map<String, String> savedMap;

            if (postgresSettingInfoFile.length() > 0) {
                savedMap = objectMapper.readValue(postgresSettingInfoFile, POSTGRES_SETTING_INFO_TYPE_REF);
            } else {
                savedMap = new HashMap<>();
            }

            savedMap.putAll(settingsToSave);
            objectMapper.writeValue(postgresSettingInfoFile, savedMap);
        } catch (Exception e) {
            throw new PropertyReadException("Error while reading settings info from file", e);
        } finally {
            postgresSettingInfoFileModificationLock.unlock();
        }
    }

    @Override
    public void deletePostgresSettingsInfos(List<String> settingsNames) throws PropertyModificationException {
        try {
            postgresSettingInfoFileModificationLock.lock();
            Map<String, String> savedMap;

            if (postgresSettingInfoFile.length() > 0) {
                savedMap = objectMapper.readValue(postgresSettingInfoFile, POSTGRES_SETTING_INFO_TYPE_REF);
            } else {
                savedMap = new HashMap<>();
            }

            settingsNames.forEach(savedMap::remove);
            objectMapper.writeValue(postgresSettingInfoFile, savedMap);
        } catch (Exception e) {
            throw new PropertyReadException("Error while reading settings info from file", e);
        } finally {
            postgresSettingInfoFileModificationLock.unlock();
        }
    }

    @Override
    public PgFacadeLoadBalancerInfo getPgFacadeLoadBalancerInfo() throws PropertyReadException {
        try {
            pgfacadeLoadBalancerInfoLock.lock();
            if (pgfacadeLoadBalancerInfoFile.length() > 0) {
                return objectMapper.readValue(pgfacadeLoadBalancerInfoFile, PgFacadeLoadBalancerInfo.class);
            } else {
                return new PgFacadeLoadBalancerInfo();
            }
        } catch (Exception e) {
            throw new PropertyReadException("Error while reading PgFacade load balancer info from file", e);
        } finally {
            pgfacadeLoadBalancerInfoLock.unlock();
        }
    }

    @Override
    public void savePgFacadeLoadBalancerInfo(PgFacadeLoadBalancerInfo info) throws PropertyModificationException {
        try {
            pgfacadeLoadBalancerInfoLock.lock();
            objectMapper.writeValue(pgfacadeLoadBalancerInfoFile, info);
        } catch (Exception e) {
            throw new PropertyReadException("Error while saving PgFacade load balancer info to file", e);
        } finally {
            pgfacadeLoadBalancerInfoLock.unlock();
        }
    }

    private File createConfigFileIfNeeded(String path) throws ConfigurationInitializationException {
        try {
            File file = new File(path);

            if (!file.exists()) {
                file.createNewFile();
            }

            if (!file.canRead() || !file.canWrite()) {
                throw new ConfigurationInitializationException("File " + file.getAbsolutePath() + " must have RW permissions for user which runs PgFacade");
            }

            return file;
        } catch (ConfigurationInitializationException configurationInitializationException) {
            throw configurationInitializationException;
        } catch (Exception e) {
            throw new ConfigurationInitializationException("Can not initialize persisted properties for Raft file storage", e);
        }
    }
}
