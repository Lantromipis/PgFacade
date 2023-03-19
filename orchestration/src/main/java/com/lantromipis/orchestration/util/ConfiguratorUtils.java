package com.lantromipis.orchestration.util;

import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.constant.PostgresConstants;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Class containing some methods to reduce code length in Configurator. Such methods shouldn't contain a lot of business logic.
 */
@ApplicationScoped
public class ConfiguratorUtils {
    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    PostgresUtils postgresUtils;

    public List<String> getDefaultPgHbaConfFileLines() {
        List<String> result = new ArrayList<>();

        result.add(PostgresConstants.PG_HBA_CONF_START_LINE);

        String subnet = platformAdapter.get().getPostgresSubnetIp();

        // "local" is for Unix domain socket connections only
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstants.PgHbaConfHost.LOCAL,
                        PostgresConstants.PG_HBA_CONF_ALL,
                        PostgresConstants.PG_HBA_CONF_ALL,
                        "\t",
                        PostgresConstants.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        // Connections between PgFacade and Postgres in their subnet
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstants.PgHbaConfHost.HOST,
                        PostgresConstants.PG_HBA_CONF_ALL,
                        PostgresConstants.PG_HBA_CONF_ALL,
                        subnet,
                        PostgresConstants.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        // Replication connections in Postgres subnet
        result.add(postgresUtils.generatePgHbaConfLine(
                        PostgresConstants.PgHbaConfHost.HOST,
                        PostgresConstants.PG_HBA_CONF_REPLICATION_DB,
                        PostgresConstants.PG_HBA_CONF_ALL,
                        subnet,
                        PostgresConstants.PgHbaConfAuthMethod.SCRAM_SHA_256
                )
        );

        return result;
    }
}
