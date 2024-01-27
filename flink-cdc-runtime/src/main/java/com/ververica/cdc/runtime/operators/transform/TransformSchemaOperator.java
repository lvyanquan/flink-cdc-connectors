/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.operators.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.schema.Selectors;
import com.ververica.cdc.common.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** A schema process function that applies user-defined transform logics. */
public class TransformSchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {

    private final List<Tuple2<String, String>> transformRules;
    private transient List<Tuple2<Selectors, Projector>> transforms;

    /** keep the relationship of TableId and table information. */
    private final Map<TableId, TableInfo> tableInfoMap;

    private final Map<TableId, TableInfo> originalTableInfoMap;

    public static TransformSchemaOperator.Builder newBuilder() {
        return new TransformSchemaOperator.Builder();
    }

    /** Builder of {@link TransformSchemaOperator}. */
    public static class Builder {
        private final List<Tuple2<String, String>> transformRules = new ArrayList<>();

        public TransformSchemaOperator.Builder addTransform(
                String tableInclusions, String projection) {
            transformRules.add(Tuple2.of(tableInclusions, projection));
            return this;
        }

        public TransformSchemaOperator build() {
            return new TransformSchemaOperator(transformRules);
        }
    }

    private TransformSchemaOperator(List<Tuple2<String, String>> transformRules) {
        this.transformRules = transformRules;
        this.tableInfoMap = new ConcurrentHashMap<>();
        this.originalTableInfoMap = new ConcurrentHashMap<>();
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        transforms =
                transformRules.stream()
                        .map(
                                tuple2 -> {
                                    String tableInclusions = tuple2.f0;
                                    String projection = tuple2.f1;

                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple2<>(
                                            selectors, Projector.generateProjector(projection));
                                })
                        .collect(Collectors.toList());
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            event = cacheLatestSchema((SchemaChangeEvent) event);
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof DataChangeEvent) {
            output.collect(new StreamRecord<>(applyDataChangeEvent(((DataChangeEvent) event))));
        }
    }

    private SchemaChangeEvent cacheLatestSchema(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        Schema originalSchema;
        Schema newSchema;
        if (event instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            originalSchema = createTableEvent.getSchema();
            event = transformCreateTableEvent(createTableEvent);
            newSchema = ((CreateTableEvent) event).getSchema();
        } else {
            TableInfo originalTableInfo = originalTableInfoMap.get(tableId);
            if (originalTableInfo == null) {
                throw new RuntimeException("Original Schema of " + tableId + " is not existed.");
            }
            TableInfo tableInfo = tableInfoMap.get(tableId);
            if (tableInfo == null) {
                throw new RuntimeException("Schema of " + tableId + " is not existed.");
            }
            originalSchema =
                    SchemaUtils.applySchemaChangeEvent(originalTableInfo.getSchema(), event);
            newSchema = SchemaUtils.applySchemaChangeEvent(tableInfo.getSchema(), event);
        }
        originalTableInfoMap.put(tableId, TableInfo.of(tableId.identifier(), originalSchema));
        tableInfoMap.put(tableId, TableInfo.of(tableId.identifier(), newSchema));
        return event;
    }

    private CreateTableEvent transformCreateTableEvent(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();
        for (Tuple2<Selectors, Projector> transform : transforms) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId)) {
                Projector projector = transform.f1;
                // update the columns of projection and add the column of projection into Schema
                return projector.applyCreateTableEvent(createTableEvent);
            }
        }
        return createTableEvent;
    }

    private DataChangeEvent applyDataChangeEvent(DataChangeEvent dataChangeEvent) {
        TableId tableId = dataChangeEvent.tableId();
        for (Tuple2<Selectors, Projector> transform : transforms) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId)) {
                Projector projector = transform.f1;
                if (projector != null && projector.isValid()) {
                    return applyProjection(projector, dataChangeEvent);
                }
            }
        }
        return dataChangeEvent;
    }

    private DataChangeEvent applyProjection(Projector projector, DataChangeEvent dataChangeEvent) {
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if (before != null) {
            BinaryRecordData data =
                    projector.recordFillDataField(
                            before,
                            originalTableInfoMap.get(dataChangeEvent.tableId()),
                            tableInfoMap.get(dataChangeEvent.tableId()));
            dataChangeEvent = DataChangeEvent.resetBefore(dataChangeEvent, data);
        }
        if (after != null) {
            BinaryRecordData data =
                    projector.recordFillDataField(
                            after,
                            originalTableInfoMap.get(dataChangeEvent.tableId()),
                            tableInfoMap.get(dataChangeEvent.tableId()));
            dataChangeEvent = DataChangeEvent.resetAfter(dataChangeEvent, data);
        }
        return dataChangeEvent;
    }
}
