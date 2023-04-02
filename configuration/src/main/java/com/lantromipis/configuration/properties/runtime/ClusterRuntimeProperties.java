package com.lantromipis.configuration.properties.runtime;

import com.lantromipis.configuration.model.RuntimePostgresInstanceInfo;
import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.constant.PostgresqlConfConstants;
import lombok.Getter;
import lombok.Setter;

import javax.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Getter
@Setter
@ApplicationScoped
//TODO scheduler to check actual properties
public class ClusterRuntimeProperties {

    private double postgresVersion = 15.1;
    private int maxPostgresConnections = 100;
    private ConcurrentMap<UUID, RuntimePostgresInstanceInfo> allPostgresInstancesInfos = new ConcurrentHashMap<>();

    public void setMaxPostgresConnections(int maxPostgresConnections) {
        this.maxPostgresConnections = maxPostgresConnections - PostgresqlConfConstants.PG_FACADE_RESERVED_CONNECTIONS_COUNT;
    }


    public RuntimePostgresInstanceInfo getPrimaryInstanceInfo() {
        return allPostgresInstancesInfos.values()
                .stream()
                .filter(info -> Boolean.TRUE.equals(info.isPrimary()))
                .findFirst()
                .orElse(null);
    }
}
