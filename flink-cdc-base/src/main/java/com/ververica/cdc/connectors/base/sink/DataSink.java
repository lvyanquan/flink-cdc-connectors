package com.ververica.cdc.connectors.base.sink;

public interface DataSink {

    EventSinkProvider getEventSinkProvider();

    MetadataApplier getMetadataApplier();
}