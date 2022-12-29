package com.lantromipis.properties.runtime;

import lombok.Getter;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ClusterRuntimeProperties {
    @Getter
    private String masterUrl = "localhost"; //TODO constant for now
    @Getter
    private int masterPort = 5432; //TODO constant for now
    @Getter
    private String defaultDatabase = "postgres"; //TODO constant for now
    @Getter
    private String pgFacadeUsername = "postgres"; //TODO constant for now
    @Getter
    private String pgFacadePassword = "postgres"; //TODO constant for now
}
