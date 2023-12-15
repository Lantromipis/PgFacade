package com.lantromipis.connectionpool.handler;

import com.lantromipis.connectionpool.model.PgChannelCleanResult;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.function.Consumer;

@ApplicationScoped
public class ConnectionPoolChannelHandlerProducer {

    public PgChannelCleaningHandler createChannelCleaningHandler(Consumer<PgChannelCleanResult> readyCallback) {
        return new PgChannelCleaningHandler(readyCallback);
    }
}
