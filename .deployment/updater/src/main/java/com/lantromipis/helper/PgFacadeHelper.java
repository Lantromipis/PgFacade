package com.lantromipis.helper;

import com.lantromipis.client.PgFacadeShutdownTemplateRestClient;
import com.lantromipis.client.model.ShutdownRaftAndOrchestrationRequestDto;
import com.lantromipis.client.model.SoftShutdownRequestDto;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class PgFacadeHelper {
    public void shutdownRaftAndOrchestration(String address, int port) throws Exception {
        try (PgFacadeShutdownTemplateRestClient restClient = createRestClient(
                PgFacadeShutdownTemplateRestClient.class,
                address,
                port,
                60000
        )) {

            restClient.shutdownRaftAndOrchestration(
                    ShutdownRaftAndOrchestrationRequestDto
                            .builder()
                            .suspend(true)
                            .build()
            );
        }
    }

    public void shutdownSoft(String address, int port, long awaitSeconds) throws Exception {
        try (PgFacadeShutdownTemplateRestClient restClient = createRestClient(
                PgFacadeShutdownTemplateRestClient.class,
                address,
                port,
                10000
        )) {

            restClient.shutdownSoft(
                    SoftShutdownRequestDto
                            .builder()
                            .shutdownPostgres(false)
                            .maxClientsAwaitPeriodSeconds(awaitSeconds)
                            .build()
            );
        }
    }

    private <T> T createRestClient(Class<T> clazz, String address, int port, long timeout) {
        URI uri = URI.create("http://" + address + ":" + port);

        return RestClientBuilder.newBuilder()
                .baseUri(uri)
                .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                .readTimeout(timeout, TimeUnit.MILLISECONDS)
                .build(clazz);
    }
}
