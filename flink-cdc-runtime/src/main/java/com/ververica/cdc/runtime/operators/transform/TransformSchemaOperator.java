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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** A schema process function that applies user-defined transform logics. */
public class TransformSchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {

    private final List<Tuple2<String, String>> transformRules;
    private transient List<Tuple2<Selectors, Optional<Projector>>> transforms;
    private final Map<TableId, TableChangeInfo> tableChangeInfoMap;
    private transient ListState<byte[]> state;

    public static TransformSchemaOperator.Builder newBuilder() {
        return new TransformSchemaOperator.Builder();
    }

    /** Builder of {@link TransformSchemaOperator}. */
    public static class Builder {
        private final List<Tuple2<String, String>> transformRules = new ArrayList<>();

        public TransformSchemaOperator.Builder addTransform(
                String tableInclusions, @Nullable String projection) {
            transformRules.add(Tuple2.of(tableInclusions, projection));
            return this;
        }

        public TransformSchemaOperator build() {
            return new TransformSchemaOperator(transformRules);
        }
    }

    private TransformSchemaOperator(List<Tuple2<String, String>> transformRules) {
        this.transformRules = transformRules;
        this.tableChangeInfoMap = new ConcurrentHashMap<>();
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
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        OperatorStateStore stateStore = context.getOperatorStateStore();
        ListStateDescriptor<byte[]> descriptor =
                new ListStateDescriptor<>("originalSchemaState", byte[].class);
        state = stateStore.getListState(descriptor);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        state.update(
                new ArrayList<>(
                        tableChangeInfoMap.values().stream()
                                .map(
                                        tableChangeInfo -> {
                                            try {
                                                return TableChangeInfo.SERIALIZER.serialize(
                                                        tableChangeInfo);
                                            } catch (IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        })
                                .collect(Collectors.toList())));
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof CreateTableEvent) {
            event = cacheCreateTable((CreateTableEvent) event);
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof SchemaChangeEvent) {
            event = cacheChangeSchema((SchemaChangeEvent) event);
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof DataChangeEvent) {
            output.collect(new StreamRecord<>(applyDataChangeEvent(((DataChangeEvent) event))));
        }
    }

    private SchemaChangeEvent cacheCreateTable(CreateTableEvent event) throws Exception {
        TableId tableId = event.tableId();
        Schema originalSchema = event.getSchema();
        event = transformCreateTableEvent(event);
        Schema newSchema = (event).getSchema();
        tableChangeInfoMap.put(tableId, TableChangeInfo.of(tableId, originalSchema, newSchema));
        return event;
    }

    private SchemaChangeEvent cacheChangeSchema(SchemaChangeEvent event) throws Exception {
        TableId tableId = event.tableId();
        TableChangeInfo tableChangeInfo = getTableChangeInfoFromState(tableId);
        Schema originalSchema =
                SchemaUtils.applySchemaChangeEvent(tableChangeInfo.getOriginalSchema(), event);
        Schema newSchema =
                SchemaUtils.applySchemaChangeEvent(tableChangeInfo.getTransformedSchema(), event);
        tableChangeInfoMap.put(tableId, TableChangeInfo.of(tableId, originalSchema, newSchema));
        return event;
    }

    private TableChangeInfo getTableChangeInfoFromState(TableId tableId) throws Exception {
        TableChangeInfo tableChangeInfo = tableChangeInfoMap.get(tableId);
        if (tableChangeInfo == null) {
            Schema originalSchema = null;
            Schema transformedSchema = null;
            Iterator<byte[]> iterator = state.get().iterator();
            while (iterator.hasNext()) {
                TableChangeInfo stateTableChangeInfo =
                        TableChangeInfo.SERIALIZER.deserialize(
                                TableChangeInfo.SERIALIZER.getVersion(), iterator.next());
                if (stateTableChangeInfo.getTableId().equals(tableId)) {
                    originalSchema = stateTableChangeInfo.getOriginalSchema();
                    transformedSchema = stateTableChangeInfo.getTransformedSchema();
                    LOG.info(
                            "TableChangeInfo state: {}",
                            stateTableChangeInfo.getTableId().identifier());
                    break;
                }
            }
            if (originalSchema == null || transformedSchema == null) {
                throw new RuntimeException(
                        "Could not find schema message from state for " + tableId);
            }
            tableChangeInfo = TableChangeInfo.of(tableId, originalSchema, transformedSchema);
        }
        return tableChangeInfo;
    }

    private CreateTableEvent transformCreateTableEvent(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();
        for (Tuple2<Selectors, Optional<Projector>> transform : transforms) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId) && transform.f1.isPresent()) {
                Projector projector = transform.f1.get();
                // update the columns of projection and add the column of projection into Schema
                return projector.applyCreateTableEvent(createTableEvent);
            }
        }
        return createTableEvent;
    }

    private DataChangeEvent applyDataChangeEvent(DataChangeEvent dataChangeEvent) throws Exception {
        TableId tableId = dataChangeEvent.tableId();
        for (Tuple2<Selectors, Optional<Projector>> transform : transforms) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId) && transform.f1.isPresent()) {
                Projector projector = transform.f1.get();
                if (projector.isValid()) {
                    return applyProjection(projector, dataChangeEvent);
                }
            }
        }
        return dataChangeEvent;
    }

    private DataChangeEvent applyProjection(Projector projector, DataChangeEvent dataChangeEvent)
            throws Exception {
        TableId tableId = dataChangeEvent.tableId();
        TableChangeInfo tableChangeInfo = getTableChangeInfoFromState(tableId);
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if (before != null) {
            BinaryRecordData data = projector.recordFillDataField(before, tableChangeInfo);
            dataChangeEvent = DataChangeEvent.resetBefore(dataChangeEvent, data);
        }
        if (after != null) {
            BinaryRecordData data = projector.recordFillDataField(after, tableChangeInfo);
            dataChangeEvent = DataChangeEvent.resetAfter(dataChangeEvent, data);
        }
        return dataChangeEvent;
    }
}
