package com.lantromipis.exception;

import com.lantromipis.model.docker.ErrorResponseDto;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

@Slf4j
@Startup
@Provider
@ApplicationScoped
public class ExceptionsMapper implements ExceptionMapper<Throwable> {
    @Override
    public Response toResponse(Throwable throwable) {
        log.error("Error.", throwable);

        if (throwable instanceof InvalidParametersException) {
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity(
                            ErrorResponseDto.builder()
                                    .message(throwable.getMessage())
                                    .timestamp(Instant.now().toString())
                                    .code(400)
                                    .build()
                    )
                    .build();
        }

        return Response
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(
                        ErrorResponseDto.builder()
                                .message(throwable.getMessage())
                                .timestamp(Instant.now().toString())
                                .code(500)
                                .build()
                )
                .build();
    }
}
