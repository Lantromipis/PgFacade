package com.lantromipis.orchestration.service.impl;

import com.lantromipis.orchestration.adapter.api.ArchiverAdapter;
import com.lantromipis.orchestration.service.api.PostgresArchiver;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

@Slf4j
@ApplicationScoped
public class PostgresArchiverImpl implements PostgresArchiver {

    @Inject
    Instance<ArchiverAdapter> archiverAdapter;

    @Override
    public void initialize() {
        log.info("Initializing archiver.");

        archiverAdapter.get().initialize();

        log.info("Archiver initialization completed.");
    }
}
