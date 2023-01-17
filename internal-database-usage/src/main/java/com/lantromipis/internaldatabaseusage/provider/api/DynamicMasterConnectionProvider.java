package com.lantromipis.internaldatabaseusage.provider.api;

import java.sql.Connection;

/**
 * Provides connections to actual Postgres master even after failover. Uses PgFacade user.
 * On failover connections to previous master must be closed, so methods which use connections from this class must be ready to retry operation in case of exception.
 */
public interface DynamicMasterConnectionProvider {

    Connection getConnection();

    void reconnectToNewMaster();
}
