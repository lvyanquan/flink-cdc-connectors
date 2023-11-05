package com.ververica.cdc.connectors.base.sink;

import io.debezium.schema.SchemaChangeEvent;

public interface MetadataApplier {

    /**
     * Called when a table schema has changed.
     *
     * @param schemaChangeEvent the table schema change event.
     */
    void applySchemaChange(SchemaChangeEvent schemaChangeEvent);
}