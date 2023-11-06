package com.ververica.cdc.connectors.base.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public interface FlinkSinkFunctionProvider extends EventSinkProvider {
    SinkFunction<Event> getSinkFunction();
}
