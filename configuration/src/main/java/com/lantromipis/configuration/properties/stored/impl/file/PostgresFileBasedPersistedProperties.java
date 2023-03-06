package com.lantromipis.configuration.properties.stored.impl.file;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lantromipis.configuration.exception.ConfigurationInitializationException;
import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.configuration.exception.PropertyModificationException;
import com.lantromipis.configuration.model.PostgresPersistedNodeInfo;
import com.lantromipis.configuration.model.PostgresPersistedSettingInfo;
import com.lantromipis.configuration.producers.FilesPathsProducer;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.configuration.properties.stored.api.PostgresPersistedProperties;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@ApplicationScoped
public class PostgresFileBasedPersistedProperties implements PostgresPersistedProperties {

    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    FilesPathsProducer filesPathsProducer;

    private ObjectMapper objectMapper;

    private File postgresNodeInfoFile;
    private File postgresSettingInfoFile;
    private ReentrantLock postgresNodeInfoFileModificationLock = new ReentrantLock();
    private ReentrantLock postgresSettingInfoFileModificationLock = new ReentrantLock();

    // @formatter:off
    private final static TypeReference<Map<UUID, PostgresPersistedNodeInfo>> POSTGRES_NODE_INFO_TYPE_REF = new TypeReference<>() {};
    private final static TypeReference<Map<String, PostgresPersistedSettingInfo>> POSTGRES_SETTING_INFO_TYPE_REF = new TypeReference<>() {};
    // @formatter:on

    @PostConstruct
    public void init() {
        objectMapper = new ObjectMapper();
        // TODO make some local identifier instead of dir

        postgresNodeInfoFile = createConfigFileIfNeeded(filesPathsProducer.getPostgresNodesInfosFilePath());
        postgresSettingInfoFile = createConfigFileIfNeeded(filesPathsProducer.getPostgresSettingsInfosFilePath());
    }

    @Override
    public List<PostgresPersistedNodeInfo> getPostgresNodeInfos() throws PropertyReadException {
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
    public PostgresPersistedNodeInfo getPostgresNodeInfo(UUID instanceId) throws PropertyReadException {
        return getPostgresNodeInfos()
                .stream()
                .filter(info -> Objects.equals(instanceId, info.getInstanceId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public void savePostgresNodeInfo(PostgresPersistedNodeInfo postgresPersistedNodeInfo) throws PropertyModificationException {
        try {
            postgresNodeInfoFileModificationLock.lock();
            Map<UUID, PostgresPersistedNodeInfo> savedMap;

            if (postgresNodeInfoFile.length() > 0) {
                savedMap = objectMapper.readValue(postgresNodeInfoFile, POSTGRES_NODE_INFO_TYPE_REF);
            } else {
                savedMap = new HashMap<>();
            }

            savedMap.put(postgresPersistedNodeInfo.getInstanceId(), postgresPersistedNodeInfo);
            objectMapper.writeValue(postgresNodeInfoFile, savedMap);
        } catch (Exception e) {
            throw new PropertyModificationException("Error while saving nodes info to file", e);
        } finally {
            postgresNodeInfoFileModificationLock.unlock();
        }
    }

    @Override
    public PostgresPersistedNodeInfo deletePostgresNodeInfo(UUID instanceId) throws PropertyModificationException {
        try {
            postgresNodeInfoFileModificationLock.lock();
            Map<UUID, PostgresPersistedNodeInfo> savedMap;

            if (postgresNodeInfoFile.length() > 0) {
                savedMap = objectMapper.readValue(postgresNodeInfoFile, POSTGRES_NODE_INFO_TYPE_REF);
            } else {
                savedMap = new HashMap<>();
            }

            PostgresPersistedNodeInfo ret = savedMap.remove(instanceId);
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
    public List<PostgresPersistedSettingInfo> getPostgresSettingInfos() throws PropertyReadException {
        try {
            postgresSettingInfoFileModificationLock.lock();
            if (postgresSettingInfoFile.length() > 0) {
                return new ArrayList<>(objectMapper.readValue(postgresSettingInfoFile, POSTGRES_SETTING_INFO_TYPE_REF).values());
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            throw new PropertyReadException("Error while reading settings info from file", e);
        } finally {
            postgresSettingInfoFileModificationLock.unlock();
        }
    }

    @Override
    public void savePostgresSettingsInfos(List<PostgresPersistedSettingInfo> persistedSettingsInfos) throws PropertyModificationException {
        try {
            postgresSettingInfoFileModificationLock.lock();
            Map<String, PostgresPersistedSettingInfo> savedMap;

            if (postgresSettingInfoFile.length() > 0) {
                savedMap = objectMapper.readValue(postgresSettingInfoFile, POSTGRES_SETTING_INFO_TYPE_REF);
            } else {
                savedMap = new HashMap<>();
            }

            savedMap.putAll(persistedSettingsInfos.stream().collect(Collectors.toMap(PostgresPersistedSettingInfo::getName, Function.identity())));
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
            Map<String, PostgresPersistedSettingInfo> savedMap;

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
            throw new ConfigurationInitializationException("Can not initialize persisted properties for FILE adapter", e);
        }
    }
}
