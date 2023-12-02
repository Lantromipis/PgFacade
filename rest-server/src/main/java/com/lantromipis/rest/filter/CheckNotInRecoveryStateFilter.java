package com.lantromipis.rest.filter;

import com.lantromipis.configuration.model.PgFacadeWorkMode;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.rest.filter.namebinding.CheckNotInRecoveryState;
import com.lantromipis.rest.model.api.error.ErrorDto;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

import java.io.IOException;
import java.time.Instant;

@Provider
@CheckNotInRecoveryState
public class CheckNotInRecoveryStateFilter implements ContainerRequestFilter {

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (PgFacadeWorkMode.RECOVERY.equals(pgFacadeRuntimeProperties.getWorkMode())) {
            requestContext.abortWith(
                    Response.
                            status(400)
                            .entity(ErrorDto
                                    .builder()
                                    .message("PgFacade is in recovery mode! ONLY recovery API can be used!")
                                    .timestamp(Instant.now())
                                    .status(400)
                                    .build()
                            )
                            .build()
            );
        }
    }
}
