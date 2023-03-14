package com.lantromipis.connectionpool.handler;

import com.lantromipis.connectionpool.handler.auth.PgChannelSaslScramSha256AuthHandler;
import com.lantromipis.connectionpool.handler.common.PgChannelAfterAuthHandler;
import com.lantromipis.connectionpool.handler.common.PgChannelStartupHandler;
import com.lantromipis.connectionpool.model.PgChannelAuthResult;
import com.lantromipis.connectionpool.model.StartupMessageInfo;
import com.lantromipis.connectionpool.model.ScramAuthInfo;
import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;

import javax.enterprise.context.ApplicationScoped;
import java.util.function.Function;

@ApplicationScoped
public class ConnectionPoolChannelHandlerProducer {

    public PgChannelStartupHandler createNewChannelStartupHandler(AuthAdditionalInfo authAdditionalInfo, StartupMessageInfo startupMessageInfo, Function<PgChannelAuthResult, Void> callbackFunction) {
        return new PgChannelStartupHandler(
                this,
                authAdditionalInfo,
                startupMessageInfo,
                callbackFunction
        );
    }

    public PgChannelSaslScramSha256AuthHandler createNewSaslScramSha256AuthHandler(ScramAuthInfo scramAuthInfo, StartupMessageInfo startupMessageInfo, Function<PgChannelAuthResult, Void> callbackFunction) {
        return new PgChannelSaslScramSha256AuthHandler(
                this,
                scramAuthInfo,
                startupMessageInfo,
                callbackFunction
        );
    }

    public PgChannelAfterAuthHandler createAfterAuthHandler(Function<PgChannelAuthResult, Void> callbackFunction) {
        return new PgChannelAfterAuthHandler(
                callbackFunction
        );
    }
}
