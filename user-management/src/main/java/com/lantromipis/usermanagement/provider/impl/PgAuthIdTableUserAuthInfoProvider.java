package com.lantromipis.usermanagement.provider.impl;

import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import com.lantromipis.usermanagement.model.UserAuthInfo;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
@Slf4j
public class PgAuthIdTableUserAuthInfoProvider implements UserAuthInfoProvider {

    @Inject
    RuntimePostgresConnectionProducer runtimePostgresConnectionProducer;

    @Inject
    PostgresProperties postgresProperties;

    private Map<String, UserAuthInfo> pgShadowTableRowsMap = new HashMap<>();

    public void initialize() {
        log.info("Initializing database users auth info using custom pg_authid table view.");
        try {
            Connection connection = runtimePostgresConnectionProducer.createNewPgFacadeUserConnectionToCurrentPrimary();

            String pgShadowSelectSql = "SELECT * FROM pg_authid WHERE rolcanlogin = true";

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(pgShadowSelectSql);

            while (resultSet.next()) {
                String username = resultSet.getString("rolname");
                String password = resultSet.getString("rolpassword");
                Date valUntil = resultSet.getDate("rolvaliduntil");

                pgShadowTableRowsMap.put(
                        username,
                        UserAuthInfo
                                .builder()
                                .username(username)
                                .passwd(password)
                                .valUntil(valUntil == null ? null : valUntil.toLocalDate())
                                .build()
                );
            }

            resultSet.close();
            statement.close();
            connection.close();

            log.info("Successfully initialized database users auth info.");

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize UserAuthInfoProvider. ", e);
        }
    }

    @Override
    public PostgresProtocolAuthenticationMethod getAuthMethodForUser(String username) {
        UserAuthInfo userAuthInfo = pgShadowTableRowsMap.get(username);
        if (userAuthInfo == null) {
            return null;
        }

        if (StringUtils.startsWith("md5", userAuthInfo.getPasswd())) {
            return PostgresProtocolAuthenticationMethod.MD5;
        } else if (StringUtils.startsWith(userAuthInfo.getPasswd(), "SCRAM-SHA-256")) {
            return PostgresProtocolAuthenticationMethod.SCRAM_SHA256;
        } else {
            return PostgresProtocolAuthenticationMethod.PLAIN_TEXT;
        }
    }

    @Override
    public String getPasswdForUser(String username) {
        UserAuthInfo userAuthInfo = pgShadowTableRowsMap.get(username);
        if (userAuthInfo == null) {
            return null;
        }

        return userAuthInfo.getPasswd();
    }
}
