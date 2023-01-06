package com.lantromipis.orchestration.mapper;

import com.lantromipis.orchestration.constant.DockerConstants;
import com.lantromipis.orchestration.model.InstanceHealth;
import com.lantromipis.orchestration.model.InstanceStatus;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DockerMapper {
    public InstanceStatus toInstanceStatus(String dockerContainerState) {
        return DockerConstants.ContainerState.RUNNING.getValue().equals(dockerContainerState)
                ? InstanceStatus.ACTIVE
                : InstanceStatus.NOT_ACTIVE;
    }

    public InstanceHealth toInstanceHealth(String dockerHealthState) {
        DockerConstants.ContainerHealth dockerHealth = DockerConstants.ContainerHealth.fromValue(dockerHealthState);

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
