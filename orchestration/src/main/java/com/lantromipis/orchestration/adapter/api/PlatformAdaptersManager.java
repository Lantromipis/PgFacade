package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.exception.InitializationException;

/**
 * Interface for PlatformAdaptersManager which manages all platform adapters.
 */
public interface PlatformAdaptersManager {
    /**
     * Initializes adapters and performs basic calls to check if initialized successfully and if configuration is valid.
     *
     * @throws InitializationException if failed to initialize
     */
    void initializeAndValidate() throws InitializationException;

    /**
     * Shutdown adapters and free all used resources.
     */
    void shutdown();
}
