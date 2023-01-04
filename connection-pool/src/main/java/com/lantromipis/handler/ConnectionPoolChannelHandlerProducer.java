package com.lantromipis.handler;

import com.lantromipis.handler.auth.PgChannelSaslScramSha256AuthHandler;
import com.lantromipis.model.ConnectionInfo;
import com.lantromipis.model.ScramAuthInfo;
import com.lantromipis.model.common.AuthAdditionalInfo;

import javax.enterprise.context.ApplicationScoped;
import java.util.function.Function;

@ApplicationScoped
public class ConnectionPoolChannelHandlerProducer {

    public PgChannelStartupHandler createNewChannelStartupHandler(AuthAdditionalInfo authAdditionalInfo, ConnectionInfo connectionInfo, Function<Boolean, Void> callbackFunction) {
        return new PgChannelStartupHandler(this, authAdditionalInfo, connectionInfo, callbackFunction);
    }

    public PgChannelSaslScramSha256AuthHandler createNewSaslScramSha256AuthHandler(ScramAuthInfo scramAuthInfo, ConnectionInfo connectionInfo, Function<Boolean, Void> callbackFunction) {
        return new PgChannelSaslScramSha256AuthHandler(scramAuthInfo, connectionInfo, callbackFunction);
    }
}
