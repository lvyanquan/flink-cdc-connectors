package com.ververica.cdc.connectors.base.sink;

public class TestDataSink implements DataSink {
    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new TestDBSink());
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return null;
    }
}
