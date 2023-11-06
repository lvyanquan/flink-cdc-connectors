package com.ververica.cdc.connectors.base.sink;

public enum OperationType {
    INSERT,
    DELETE,
    UPDATE;

    public String shortString() {
        return "";
    }
}
