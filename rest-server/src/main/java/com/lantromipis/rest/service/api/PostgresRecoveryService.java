package com.lantromipis.rest.service.api;

import com.lantromipis.rest.model.internal.PostgresRestoreSettings;

public interface PostgresRecoveryService {
    void startRecoveryFromBackup(PostgresRestoreSettings settings);
}
