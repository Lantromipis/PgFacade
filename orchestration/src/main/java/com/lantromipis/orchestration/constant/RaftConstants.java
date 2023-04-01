package com.lantromipis.orchestration.constant;

public class RaftConstants {

    // chunks
    public static final String POSTGRES_NODES_INFO_CHUNK = "nodeInfosChunk";
    public static final String POSTGRES_SETTINGS_INFO_CHUNK = "settingsInfosChunk";
    public static final String POSTGRES_ARCHIVE_INFO_CHUNK = "archiveInfosChunk";

    // events
    public static final String NOTIFY_ALL_CLUSTER_ABOUT_SWITCHOVER_STARTED = "NACASS";
    public static final String NOTIFY_ALL_CLUSTER_ABOUT_SWITCHOVER_COMPLETED = "NACASC";

    // node info
    public static final String SAVE_POSTGRES_NODE_INFO = "SPNI";
    public static final String DELETE_POSTGRES_NODE_INFO = "DPNI";
    public static final String CLEAR_POSTGRES_NODES_INFOS = "CPNI";

    // settings info
    public static final String SAVE_POSTGRES_SETTINGS_INFO = "SPSI";

    // archive
    public static final String SAVE_POSTGRES_ARCHIVE_INFO = "SPAI";


    // other
    public static final String DUMMY_COMMIT_TEST_COMMAND = "DCTC";

    private RaftConstants() {
    }
}
