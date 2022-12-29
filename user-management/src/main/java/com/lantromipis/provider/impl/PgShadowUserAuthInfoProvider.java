package com.lantromipis.provider.impl;

import com.lantromipis.properties.runtime.ClusterRuntimeProperties;
import com.lantromipis.provider.api.UserAuthInfoProvider;
import com.lantromipis.model.AuthenticationMethod;
import com.lantromipis.model.PgShadowTableRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
@Slf4j
public class PgShadowUserAuthInfoProvider implements UserAuthInfoProvider {

    @Inject
    ClusterRuntimeProperties clusterRuntimeProperties;

    private Map<String, PgShadowTableRow> pgShadowTableRowsMap = new HashMap<>();

    public void initialize() {
        log.info("Initializing database users auth info using pg_shadow table");
        try {
            String jdbcUrl =
                    "jdbc:postgresql://"
                            + clusterRuntimeProperties.getMasterUrl()
                            + ":"
                            + clusterRuntimeProperties.getMasterPort()
                            + "/"
                            + clusterRuntimeProperties.getDefaultDatabase();

            Connection connection = DriverManager.getConnection(
                    jdbcUrl,
                    clusterRuntimeProperties.getPgFacadeUsername(),
                    clusterRuntimeProperties.getPgFacadePassword()
            );

            String pgShadowSelectSql = "select * from pg_shadow;";

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(pgShadowSelectSql);

            while (resultSet.next()) {
                String username = resultSet.getString("usename");
                String password = resultSet.getString("passwd");
                Date valUntil = resultSet.getDate("valuntil");

                pgShadowTableRowsMap.put(
                        username,
                        PgShadowTableRow
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
    public AuthenticationMethod getAuthMethodForUser(String username) {
        PgShadowTableRow pgShadowTableRow = pgShadowTableRowsMap.get(username);
        if (pgShadowTableRow == null) {
            return null;
        }

        if (StringUtils.startsWith("md5", pgShadowTableRow.getPasswd())) {
            return AuthenticationMethod.MD5;
        } else if (StringUtils.startsWith(pgShadowTableRow.getPasswd(), "SCRAM-SHA-256")) {
            return AuthenticationMethod.SCRAM_SHA256;
        } else {
            return AuthenticationMethod.PLAIN_TEXT;
        }
    }

    @Override
    public String getPasswdForUser(String username) {
        PgShadowTableRow pgShadowTableRow = pgShadowTableRowsMap.get(username);
        if (pgShadowTableRow == null) {
            return null;
        }

        return pgShadowTableRow.getPasswd();
    }
}
