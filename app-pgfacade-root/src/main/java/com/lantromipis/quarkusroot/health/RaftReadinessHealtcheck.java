package com.lantromipis.quarkusroot.health;

import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

@Readiness
@ApplicationScoped
public class RaftReadinessHealtcheck implements HealthCheck {

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder responseBuilder = HealthCheckResponse.named(PgFacadeConstants.RAFT_SERVER_UP_READINESS_CHECK);

        if (pgFacadeRuntimeProperties.isRaftServerUp()) {
            responseBuilder.up();
        } else {
            responseBuilder.down();
        }

        return responseBuilder.build();
    }
}
