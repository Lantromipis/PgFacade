package com.lantromipis.configuration.properties.runtime;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.model.PgFacadeWorkMode;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ApplicationScoped
public class PgFacadeRuntimeProperties {
    private PgFacadeRaftRole raftRole;
    private boolean raftServerUp = false;
    private int httpPort = 8080;
    private boolean pgFacadeOrchestrationForceDisabled = false;
    private PgFacadeWorkMode workMode = PgFacadeWorkMode.OPERATIONAL;
}
