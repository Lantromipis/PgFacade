package com.lantromipis.internaldatabaseusage.provider.impl;

import com.lantromipis.configuration.event.MasterReadyEvent;
import com.lantromipis.configuration.predefined.PostgresProperties;
import com.lantromipis.configuration.runtime.ClusterRuntimeProperties;
import com.lantromipis.internaldatabaseusage.provider.api.DynamicMasterConnectionProvider;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.sql.Connection;

@Slf4j
@ApplicationScoped
public class DynamicHikariConnectionProvider implements DynamicMasterConnectionProvider {

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    @Inject
    PostgresProperties postgresProperties;

    private HikariDataSource hikariDataSource;

    @Override
    public Connection getConnection() {
        try {
            return hikariDataSource.getConnection();
        } catch (Exception e) {
            log.error("Error while acquiring connection to master.");
            return null;
        }
    }

    @Override
    public void reconnectToNewMaster() {
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }

        HikariConfig hikariConfig = new HikariConfig();

        String jdbcUrl =
                "jdbc:postgresql://"
                        + clusterRuntimeProperties.getMasterHostAddress()
                        + ":"
                        + clusterRuntimeProperties.getMasterPort()
                        + "/"
                        + postgresProperties.pgFacadeDatabase();

        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(postgresProperties.pgFacadeUser());
        hikariConfig.setPassword(postgresProperties.pgFacadePassword());
        hikariConfig.setMaximumPoolSize(5);

        hikariDataSource = new HikariDataSource(hikariConfig);
    }

    public void listenToMasterReadyEvent(@Observes MasterReadyEvent masterReadyEvent) {
        reconnectToNewMaster();
    }
}
