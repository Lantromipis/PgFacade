package com.lantromipis.configuration.properties.runtime;

import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Getter
@Setter
@ApplicationScoped
//TODO scheduler to check actual properties
public class ClusterRuntimeProperties {

    private double postgresVersion = 15.1;
    private int maxPostgresConnections = 100;
    private ConcurrentMap<UUID, RuntimePostgresInstanceInfo> allPostgresInstancesInfos = new ConcurrentHashMap<>();

    public void setMaxPostgresConnections(int maxPostgresConnections) {
        this.maxPostgresConnections = maxPostgresConnections;
        log.info("Setting max pool connection limit per instance to {} (max_connections - superuser_reserved_connections)", this.maxPostgresConnections);
    }


    public RuntimePostgresInstanceInfo getPrimaryInstanceInfo() {
        return allPostgresInstancesInfos.values()
                .stream()
                .filter(info -> Boolean.TRUE.equals(info.isPrimary()))
                .findFirst()
                .orElse(null);
    }
}
