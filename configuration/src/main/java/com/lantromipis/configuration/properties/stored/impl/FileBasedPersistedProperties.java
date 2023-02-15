package com.lantromipis.configuration.properties.stored.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lantromipis.configuration.exception.ConfigurationInitializationException;
import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.configuration.exception.PropertyModifyException;
import com.lantromipis.configuration.model.PostgresPersistedNodeInfo;
import com.lantromipis.configuration.properties.predefined.StorageProperties;
import com.lantromipis.configuration.properties.stored.api.PersistedProperties;
import io.quarkus.arc.lookup.LookupIfProperty;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@ApplicationScoped
@LookupIfProperty(name = "pg-facade.storage.adapter", stringValue = "file")
public class FileBasedPersistedProperties implements PersistedProperties {

    @Inject
    StorageProperties storageProperties;

    private ObjectMapper objectMapper;

    private final static String JSON_EXTENSION = ".json";

    private File postgresNodeInfoFile;
    private ReentrantLock postgresNodeInfoFileModificationLock = new ReentrantLock();

    // @formatter:off
    private final static TypeReference<Map<UUID, PostgresPersistedNodeInfo>> POSTGRES_NODE_INFO_TYPE_REF = new TypeReference<>() {};
    // @formatter:on

    @PostConstruct
    public void init() {
        objectMapper = new ObjectMapper();
        postgresNodeInfoFile = createConfigFileIfNeeded(
                storageProperties.file().directoryPath()
                        + "/"
                        + storageProperties.file().postgresNodesInfoFilename() + JSON_EXTENSION
        );
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
    public void savePostgresNodeInfo(PostgresPersistedNodeInfo postgresPersistedNodeInfo) throws PropertyModifyException {
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
            throw new PropertyModifyException("Error while saving nodes info to file", e);
        } finally {
            postgresNodeInfoFileModificationLock.unlock();
        }
    }

    @Override
    public PostgresPersistedNodeInfo deletePostgresNodeInfo(UUID instanceId) throws PropertyModifyException {
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
            throw new PropertyModifyException("Error while saving nodes info to file", e);
        } finally {
            postgresNodeInfoFileModificationLock.unlock();
        }
    }

    private File createConfigFileIfNeeded(String path) throws ConfigurationInitializationException {
        try {
            File file = new File(path);

            if (!file.exists()) {
                Files.createDirectories(Paths.get(storageProperties.file().directoryPath()));
                file.createNewFile();
            }

            if (!file.canRead() || !file.canWrite()) {
                throw new ConfigurationInitializationException("File " + file.getAbsolutePath() + " must have RW permissions for user which runs PgFacade");
            }

            return file;
        } catch (Exception e) {
            throw new ConfigurationInitializationException("Can not initialize persisted properties for FILE adapter", e);
        }
    }
}
