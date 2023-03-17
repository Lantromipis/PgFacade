package com.lantromipis.quarkusroot.validator;

import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@Slf4j
@ApplicationScoped
public class OrchestratorConfigurationValidator implements ConfigurationValidator {
    @Inject
    OrchestrationProperties orchestrationProperties;

    @Override
    public boolean validate() {
        boolean flag = true;
        if (orchestrationProperties.common().standby().count() < 1 && !OrchestrationProperties.AdapterType.NO_ADAPTER.equals(orchestrationProperties.adapter())) {
            log.error("Invalid configuration. Standby count must be greater than 1 if any adapter is used. If you are planning to use PgFacade as proxy + connection pool, set orchestration adapter to 'no'.");
            flag = false;
        }
        return flag;
    }
}
