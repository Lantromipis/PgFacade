package com.lantromipis.connectionpool.handler;

import com.lantromipis.connectionpool.handler.auth.PgChannelSaslScramSha256AuthHandler;
import com.lantromipis.connectionpool.handler.common.PgChannelAfterAuthHandler;
import com.lantromipis.connectionpool.handler.common.PgChannelStartupHandler;
import com.lantromipis.connectionpool.handler.common.PgChannelCleaningHandler;
import com.lantromipis.connectionpool.model.PgChannelAuthResult;
import com.lantromipis.connectionpool.model.PgChannelCleanResult;
import com.lantromipis.connectionpool.model.StartupMessageInfo;
import com.lantromipis.connectionpool.model.auth.ScramAuthInfo;
import com.lantromipis.connectionpool.model.auth.AuthAdditionalInfo;

import javax.enterprise.context.ApplicationScoped;
import java.util.function.Consumer;
import java.util.function.Function;

@ApplicationScoped
public class ConnectionPoolChannelHandlerProducer {

    public PgChannelCleaningHandler createChannelCleaningHandler(Consumer<PgChannelCleanResult> readyCallback) {
        return new PgChannelCleaningHandler(readyCallback);
    }

    public PgChannelStartupHandler createNewChannelStartupHandler(AuthAdditionalInfo authAdditionalInfo, StartupMessageInfo startupMessageInfo, Consumer<PgChannelAuthResult> callbackFunction) {
        return new PgChannelStartupHandler(
                this,
                authAdditionalInfo,
                startupMessageInfo,
                callbackFunction
        );
    }

    public PgChannelSaslScramSha256AuthHandler createNewSaslScramSha256AuthHandler(ScramAuthInfo scramAuthInfo, StartupMessageInfo startupMessageInfo, Consumer<PgChannelAuthResult> callbackFunction) {
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
