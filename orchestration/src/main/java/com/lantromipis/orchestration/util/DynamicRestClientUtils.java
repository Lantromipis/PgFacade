package com.lantromipis.orchestration.util;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class DynamicRestClientUtils {
    public <T> T createRestClient(Class<T> clazz, String address, int port) {
        URI uri = URI.create("http://" + address + ":" + port);

        return RestClientBuilder.newBuilder()
                .baseUri(uri)
                .connectTimeout(1000, TimeUnit.MILLISECONDS)
                .readTimeout(1000, TimeUnit.MILLISECONDS)
                .build(clazz);
    }

    public <T> T createRestClient(Class<T> clazz, String address, int port, long timeoutMs) {
        URI uri = URI.create("http://" + address + ":" + port);

        return RestClientBuilder.newBuilder()
                .baseUri(uri)
                .connectTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                .readTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                .build(clazz);
    }

    public void closeClient(Closeable client) {
        if (client == null) {
            return;
        }

        try {
            client.close();
        } catch (Exception ignored) {
        }
    }
}
