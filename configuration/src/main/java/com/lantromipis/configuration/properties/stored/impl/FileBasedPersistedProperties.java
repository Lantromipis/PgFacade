package com.lantromipis.configuration.properties.stored.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lantromipis.configuration.exception.PropertyReadException;
import com.lantromipis.configuration.exception.PropertySaveException;
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
import java.util.stream.Collectors;

@Slf4j
@ApplicationScoped
@LookupIfProperty(name = "pg-facade.storage.adapter", stringValue = "file")
public class FileBasedPersistedProperties implements PersistedProperties {

    @Inject
    StorageProperties storageProperties;

    private ObjectMapper objectMapper;

    private final static String JSON_EXTENSION = ".json";

    // @formatter:off
    private final static TypeReference<Map<UUID, PostgresPersistedNodeInfo>> POSTGRES_NODE_INFO_TYPE_REF = new TypeReference<>() {};
    // @formatter:on

    @PostConstruct
    public void inti() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public List<PostgresPersistedNodeInfo> getPostgresNodeInfos() throws PropertyReadException {
        File file = new File(
                storageProperties.file().directoryPath()
                        + "/"
                        + storageProperties.file().postgresNodesInfoFilename() + JSON_EXTENSION
        );

        if (file.exists()) {
            try {
                return new ArrayList<>(objectMapper.readValue(file, POSTGRES_NODE_INFO_TYPE_REF).values());
            } catch (Exception e) {
                throw new PropertyReadException("Error while reading nodes info from file", e);
            }
        } else {
            return Collections.emptyList();
        }
    }

    //only one thread can modify file, so we don't lose updates.
    @Override
    public synchronized void savePostgresNodeInfo(PostgresPersistedNodeInfo postgresPersistedNodeInfo) throws PropertySaveException {
        File file = new File(
                storageProperties.file().directoryPath()
                        + "/"
                        + storageProperties.file().postgresNodesInfoFilename() + JSON_EXTENSION
        );

        try {
            Map<UUID, PostgresPersistedNodeInfo> savedMap;

            if (!file.exists()) {
                savedMap = new HashMap<>();
                Files.createDirectories(Paths.get(storageProperties.file().directoryPath()));
                file.createNewFile();
            } else {
                savedMap = objectMapper.readValue(file, POSTGRES_NODE_INFO_TYPE_REF);
            }

            savedMap.put(postgresPersistedNodeInfo.getInstanceId(), postgresPersistedNodeInfo);
            objectMapper.writeValue(file, savedMap);
        } catch (Exception e) {
            throw new PropertySaveException("Error while persisting nodes info to file", e);
        }
    }
}
