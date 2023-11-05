package com.ververica.cdc.connectors.base.sink;

public interface Event {
    TableID tableId();
}
