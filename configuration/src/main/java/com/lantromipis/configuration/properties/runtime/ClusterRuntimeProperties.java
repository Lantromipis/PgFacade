package com.lantromipis.configuration.properties.runtime;

import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Getter
@Setter
@ApplicationScoped
//TODO scheduler to check actual properties
public class ClusterRuntimeProperties {
    private ConcurrentMap<UUID, RuntimePostgresInstanceInfo> allPostgresInstancesInfos = new ConcurrentHashMap<>();

    public RuntimePostgresInstanceInfo getPrimaryInstanceInfo() {
        return getPrimaryInstanceInfoOptional()
                .orElse(null);
    }

    public Optional<RuntimePostgresInstanceInfo> getPrimaryInstanceInfoOptional() {
        return allPostgresInstancesInfos.values()
                .stream()
                .filter(info -> Boolean.TRUE.equals(info.isPrimary()))
                .findFirst();
    }

    public RuntimePostgresInstanceInfo getInstanceInfo(UUID instanceId) {
        return allPostgresInstancesInfos.get(instanceId);
    }

    public UUID getPrimaryInstanceId() {
        return getPrimaryInstanceInfoOptional()
                .map(RuntimePostgresInstanceInfo::getInstanceId)
                .orElse(null);
    }
}
