package com.lantromipis.configuration.logging;

import com.lantromipis.configuration.constants.MDCConstants;
import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import io.quarkus.logging.LoggingFilter;
import org.slf4j.MDC;

import javax.inject.Inject;
import java.util.logging.Filter;
import java.util.logging.LogRecord;

@LoggingFilter(name = "custom-filter")
public class CustomLoggingFilter implements Filter {
    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Override
    public boolean isLoggable(LogRecord record) {
        PgFacadeRaftRole raftRole = pgFacadeRuntimeProperties.getRaftRole();
        if (raftRole != null) {
            MDC.put(MDCConstants.RAFT_ROLE, raftRole.getMdcValue());
        }
        return true;
    }
}
