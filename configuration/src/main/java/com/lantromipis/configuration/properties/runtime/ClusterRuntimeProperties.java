package com.lantromipis.configuration.properties.runtime;

import lombok.Getter;
import lombok.Setter;

import javax.enterprise.context.ApplicationScoped;

@Getter
@Setter
@ApplicationScoped
public class ClusterRuntimeProperties {
    private String masterHostAddress = "localhost";
    private int masterPort = 5432; //TODO constant for now
    private int maxPostgresConnections = 100;
}
