package com.lantromipis.configuration.properties.runtime;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import lombok.Getter;
import lombok.Setter;

import javax.enterprise.context.ApplicationScoped;

@Getter
@Setter
@ApplicationScoped
public class PgFacadeRuntimeProperties {
    private PgFacadeRaftRole raftRole;
}
