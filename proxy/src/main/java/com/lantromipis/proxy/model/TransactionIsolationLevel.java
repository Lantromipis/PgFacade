package com.lantromipis.proxy.model;


public enum TransactionIsolationLevel {
    SERIALIZABLE,
    REPEATABLE_READ,
    READ_COMMITTED,
    READ_UNCOMMITTED;
}
