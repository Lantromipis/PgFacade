package com.lantromipis.usermanagement.provider.impl;

import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.predefined.PostgresProperties;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolScramConstants;
import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import com.lantromipis.usermanagement.model.ScramSha256UserAuthInfo;
import com.lantromipis.usermanagement.model.UserAuthInfo;
import com.lantromipis.usermanagement.provider.api.UserAuthInfoProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

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

            String pgShadowSelectSql = "SELECT * FROM pg_authid WHERE rolcanlogin = TRUE";

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(pgShadowSelectSql);

            while (resultSet.next()) {
                String username = resultSet.getString("rolname");
                String password = resultSet.getString("rolpassword");
                Date valUntil = resultSet.getDate("rolvaliduntil");

                pgShadowTableRowsMap.put(
                        username,
                        createUserAuthInfo(username, password, valUntil)
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

        return userAuthInfo.getAuthenticationMethod();
    }

    @Override
    public String getPasswdForUser(String username) {
        UserAuthInfo userAuthInfo = pgShadowTableRowsMap.get(username);
        if (userAuthInfo == null) {
            return null;
        }

        return userAuthInfo.getPasswd();
    }

    @Override
    public UserAuthInfo getUserAuthInfo(String username) {
        return pgShadowTableRowsMap.get(username);
    }

    private PostgresProtocolAuthenticationMethod getAuthMethodFromPasswd(String passwd) {
        if (StringUtils.startsWith(passwd, "SCRAM-SHA-256")) {
            return PostgresProtocolAuthenticationMethod.SCRAM_SHA256;
        } else if (StringUtils.startsWith(passwd, "md5")) {
            return PostgresProtocolAuthenticationMethod.MD5;
        } else {
            return PostgresProtocolAuthenticationMethod.PLAIN_TEXT;
        }
    }

    private UserAuthInfo createUserAuthInfo(String username, String passwd, Date valUntil) {
        PostgresProtocolAuthenticationMethod authMethod = getAuthMethodFromPasswd(passwd);

        UserAuthInfo ret;
        switch (authMethod) {
            case SCRAM_SHA256 -> {
                ScramSha256UserAuthInfo scramSha256UserAuthInfo = new ScramSha256UserAuthInfo();

                Matcher passwdMatcher = PostgresProtocolScramConstants.SCRAM_SHA_256_PASSWD_FORMAT_PATTERN.matcher(passwd);
                passwdMatcher.matches();

                scramSha256UserAuthInfo.setIterationCount(Integer.parseInt(passwdMatcher.group(1)));
                scramSha256UserAuthInfo.setSalt(passwdMatcher.group(2));

                String storedKey = passwdMatcher.group(3);
                String serverKey = passwdMatcher.group(4);

                scramSha256UserAuthInfo.setStoredKey(storedKey);
                scramSha256UserAuthInfo.setServerKey(serverKey);

                scramSha256UserAuthInfo.setStoredKeyDecodedBytes(Base64.getDecoder().decode(storedKey));
                scramSha256UserAuthInfo.setServerKeyDecodedBytes(Base64.getDecoder().decode(serverKey));

                ret = scramSha256UserAuthInfo;
            }
            default -> ret = new UserAuthInfo();
        }

        ret.setUsername(username);
        ret.setPasswd(passwd);
        ret.setValUntil(valUntil == null ? null : valUntil.toLocalDate());
        ret.setAuthenticationMethod(authMethod);

        return ret;
    }
}
