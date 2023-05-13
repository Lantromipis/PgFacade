package com.lantromipis.health;

import jakarta.enterprise.context.ApplicationScoped;
import lombok.Getter;
import lombok.Setter;
import org.eclipse.microprofile.health.*;

@Liveness
@ApplicationScoped
public class ProxyHostsKnownLivelinessHealtcheck implements HealthCheck {

    @Getter
    @Setter
    private boolean ready = true;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder responseBuilder = HealthCheckResponse.named("Proxy hosts known");

        if (ready) {
            responseBuilder.up();
        } else {
            responseBuilder.down();
        }

        return responseBuilder.build();
    }
}
