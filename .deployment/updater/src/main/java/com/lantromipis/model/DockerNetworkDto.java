package com.lantromipis.model;

import lombok.Data;

@Data
public class DockerNetworkDto {
    private String networkName;
    private boolean create;
}
