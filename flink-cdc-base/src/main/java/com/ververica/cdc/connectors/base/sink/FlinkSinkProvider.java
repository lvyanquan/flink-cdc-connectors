package com.ververica.cdc.connectors.base.sink;

import org.apache.flink.api.connector.sink2.Sink;

public interface FlinkSinkProvider extends EventSinkProvider {

    static FlinkSinkProvider of(Sink<Event> sink) {
        return () -> sink;
    }

    Sink<Event> getSink();
}
