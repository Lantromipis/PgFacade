package com.lantromipis.connectionpool.handler;

import com.lantromipis.connectionpool.handler.auth.PgChannelSaslScramSha256AuthHandler;
import com.lantromipis.connectionpool.handler.common.PgChannelAfterAuthHandler;
import com.lantromipis.connectionpool.handler.common.PgChannelCleaningHandler;
import com.lantromipis.connectionpool.handler.common.PgChannelStartupHandler;
import com.lantromipis.connectionpool.model.PgChannelAuthResult;
import com.lantromipis.connectionpool.model.PgChannelCleanResult;
import com.lantromipis.connectionpool.model.StartupMessageInfo;
import com.lantromipis.connectionpool.model.auth.PoolAuthInfo;
import com.lantromipis.connectionpool.model.auth.ScramPoolAuthInfo;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.function.Consumer;

@ApplicationScoped
public class ConnectionPoolChannelHandlerProducer {

    public PgChannelCleaningHandler createChannelCleaningHandler(Consumer<PgChannelCleanResult> readyCallback) {
        return new PgChannelCleaningHandler(readyCallback);
    }

    public PgChannelStartupHandler createNewChannelStartupHandler(PoolAuthInfo poolAuthInfo, StartupMessageInfo startupMessageInfo, Consumer<PgChannelAuthResult> callbackFunction) {
        return new PgChannelStartupHandler(
                this,
                poolAuthInfo,
                startupMessageInfo,
                callbackFunction
        );
    }

    public PgChannelSaslScramSha256AuthHandler createNewSaslScramSha256AuthHandler(ScramPoolAuthInfo scramAuthInfo, StartupMessageInfo startupMessageInfo, Consumer<PgChannelAuthResult> callbackFunction) {
        return new PgChannelSaslScramSha256AuthHandler(
                this,
                scramAuthInfo,
                startupMessageInfo,
                callbackFunction
        );
    }

    public PgChannelAfterAuthHandler createAfterAuthHandler(Consumer<PgChannelAuthResult> callbackFunction) {
        return new PgChannelAfterAuthHandler(
                callbackFunction
        );
    }
}
