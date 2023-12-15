package com.lantromipis.postgresprotocol.producer;

import com.lantromipis.postgresprotocol.handler.frontend.PgChannelAfterAuthHandler;
import com.lantromipis.postgresprotocol.handler.frontend.PgChannelStartupHandler;
import com.lantromipis.postgresprotocol.handler.frontend.auth.PgChannelSaslScramSha256AuthHandler;
import com.lantromipis.postgresprotocol.model.internal.PgChannelAuthResult;
import com.lantromipis.postgresprotocol.model.internal.auth.PgAuthInfo;
import com.lantromipis.postgresprotocol.model.internal.auth.ScramPgAuthInfo;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Map;
import java.util.function.Consumer;

@ApplicationScoped
public class PgFrontendChannelHandlerProducer {

    public PgChannelStartupHandler createNewChannelStartupHandler(PgAuthInfo pgAuthInfo, Map<String, String> startupParameters, Consumer<PgChannelAuthResult> callbackFunction) {
        return new PgChannelStartupHandler(
                this,
                pgAuthInfo,
                startupParameters,
                callbackFunction
        );
    }

    public PgChannelSaslScramSha256AuthHandler createNewSaslScramSha256AuthHandler(ScramPgAuthInfo scramAuthInfo, Consumer<PgChannelAuthResult> callbackFunction) {
        return new PgChannelSaslScramSha256AuthHandler(
                this,
                scramAuthInfo,
                callbackFunction
        );
    }

    public PgChannelAfterAuthHandler createAfterAuthHandler(Consumer<PgChannelAuthResult> callbackFunction) {
        return new PgChannelAfterAuthHandler(
                callbackFunction
        );
    }
}
