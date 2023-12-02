package com.lantromipis.quarkusroot.validator;

import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import com.lantromipis.orchestration.util.DockerUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;

@Slf4j
@ApplicationScoped
public class OrchestratorConfigurationValidator implements ConfigurationValidator {
    @Inject
    OrchestrationProperties orchestrationProperties;

    @Inject
    DockerUtils dockerUtils;

    @Override
    public boolean validate() {
        boolean flag = true;

        if (orchestrationProperties.common().standby().count() < 1 && !OrchestrationProperties.AdapterType.NO_ADAPTER.equals(orchestrationProperties.adapter())) {
            log.error("Invalid configuration. Standby count must be greater than 1 if any adapter is used. If you are planning to use PgFacade as proxy + connection pool, set orchestration adapter to 'no'.");
            flag = false;
        }

        if (!validateForDocker()) {
            flag = false;
        }

        return flag;
    }

    private boolean validateForDocker() {
        if (!OrchestrationProperties.AdapterType.DOCKER.equals(orchestrationProperties.adapter())) {
            return true;
        }

        boolean flag = true;

        if (!validateDockerResourcesProperties(orchestrationProperties.docker().externalLoadBalancer().resources(), "external load balancer")) {
            flag = false;
        }

        if (!validateDockerResourcesProperties(orchestrationProperties.docker().pgFacade().resources(), "PgFacade")) {
            flag = false;
        }

        if (!validateDockerResourcesProperties(orchestrationProperties.docker().postgres().resources(), "Postgres")) {
            flag = false;
        }

        return flag;
    }

    private boolean validateDockerResourcesProperties(OrchestrationProperties.DockerProperties.DockerContainerResources resources, String settingLogName) {
        boolean flag = true;

        if (resources.cpuLimit() == null || BigDecimal.ZERO.equals(resources.cpuLimit())) {
            log.error("Invalid docker adapter configuration. CpuLimit for {} can not be null or 0", settingLogName);
            flag = false;
        }

        if (StringUtils.isEmpty(resources.memoryLimit())) {
            log.error("Invalid docker adapter configuration. Memory limit for {} can not empty", settingLogName);
            flag = false;
        }

        if (!dockerUtils.validateMemoryString(resources.memoryLimit())) {
            log.error("Invalid docker adapter configuration. Memory limit for {} is invalid. Use int value with unit without space. Possible unis: {}", settingLogName, dockerUtils.getPossibleMemoryUnits().toString());
            flag = false;
        }

        return flag;
    }
}
