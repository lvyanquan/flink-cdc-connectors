package com.ververica.cdc.connectors.base.sink;

public enum OperationType {
    INSERT,
    DELETE,
    UPDATE_BEFORE,
    UPDATE_AFTER;

    public String shortString() {
        return "";
    }
}
