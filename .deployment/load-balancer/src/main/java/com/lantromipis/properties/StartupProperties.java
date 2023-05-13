package com.lantromipis.properties;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "balancer.startup")
public interface StartupProperties {
    String initialHttpHost();
    int initialHttpPort();
}
