package com.lantromipis.handler;

import com.lantromipis.handler.auth.ChannelSaslScramSha256AuthHandler;
import com.lantromipis.model.ConnectionInfo;
import com.lantromipis.model.ScramAuthInfo;
import com.lantromipis.model.common.AuthAdditionalInfo;
import com.lantromipis.provider.api.UserAuthInfoProvider;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.function.Function;

@ApplicationScoped
public class ConnectionPoolChannelHandlerProducer {

    @Inject
    UserAuthInfoProvider userAuthInfoProvider;

    public ChannelStartupHandler createNewChannelStartupHandler(AuthAdditionalInfo authAdditionalInfo, ConnectionInfo connectionInfo, Function<Boolean, Void> callbackFunction) {
        return new ChannelStartupHandler(this, authAdditionalInfo, connectionInfo, callbackFunction);
    }

    public ChannelSaslScramSha256AuthHandler createNewSaslScramSha256AuthHandler(ScramAuthInfo scramAuthInfo, ConnectionInfo connectionInfo, Function<Boolean, Void> callbackFunction) {
        return new ChannelSaslScramSha256AuthHandler(scramAuthInfo, connectionInfo, callbackFunction);
    }
}
