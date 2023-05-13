package com.lantromipis.properties;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "updater")
public interface UpdaterProperties {

    DockerProperties docker();

    interface DockerProperties {
        String host();
    }
}
