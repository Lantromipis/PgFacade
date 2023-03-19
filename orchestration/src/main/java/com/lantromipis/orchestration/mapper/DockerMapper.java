package com.lantromipis.orchestration.mapper;

import com.github.dockerjava.api.command.HealthState;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.lantromipis.orchestration.constant.DockerConstants;
import com.lantromipis.orchestration.model.InstanceHealth;
import com.lantromipis.orchestration.model.InstanceStatus;

import javax.enterprise.context.ApplicationScoped;
import java.util.Optional;

@ApplicationScoped
public class DockerMapper {

    public InstanceHealth toInstanceHealth(InspectContainerResponse inspectContainerResponse) {
        DockerConstants.ContainerHealth dockerHealth = Optional.of(inspectContainerResponse.getState())
                .map(InspectContainerResponse.ContainerState::getHealth)
                .map(HealthState::getStatus)
                .map(DockerConstants.ContainerHealth::fromValue)
                .orElse(null);

        if (dockerHealth == null) {
            return null;
        }

        switch (dockerHealth) {
            case STARTING -> {
                return InstanceHealth.STARTING;
            }
            case HEALTHY -> {
                return InstanceHealth.HEALTHY;
            }
            case NONE, UNHEALTHY -> {
                return InstanceHealth.UNHEALTHY;
            }
            default -> {
                return null;
            }
        }
    }
}
