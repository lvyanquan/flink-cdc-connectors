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

package com.ververica.cdc.runtime.partitioning;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.OperationType;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.runtime.operators.sink.SchemaEvolutionClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Operator for processing events from {@link
 * com.ververica.cdc.runtime.operators.schema.SchemaOperator} before {@link EventPartitioner}.
 */
@Internal
public class PrePartitionOperator extends AbstractStreamOperator<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent> {

    private final OperatorID schemaOperatorId;
    private final int downstreamParallelism;

    private SchemaEvolutionClient schemaEvolutionClient;
    private final Map<TableId, Function<DataChangeEvent, Integer>> cachedHashFunctions =
            new HashMap<>();

    public PrePartitionOperator(OperatorID schemaOperatorId, int downstreamParallelism) {
        this.schemaOperatorId = schemaOperatorId;
        this.downstreamParallelism = downstreamParallelism;
    }

    @Override
    public void open() throws Exception {
        super.open();
        TaskOperatorEventGateway toCoordinator =
                getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway();
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, schemaOperatorId);
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            // Update hash function
            recreateHashFunction(((SchemaChangeEvent) event).tableId());
            // Broadcast SchemaChangeEvent
            broadcastEvent(event);
        } else if (event instanceof FlushEvent) {
            // Broadcast FlushEvent
            broadcastEvent(event);
        } else if (event instanceof DataChangeEvent) {
            // Partition DataChangeEvent by table ID and primary keys
            partitionBy(((DataChangeEvent) event));
        }
    }

    private void partitionBy(DataChangeEvent dataChangeEvent) {
        Function<DataChangeEvent, Integer> hashFunction =
                cachedHashFunctions.get(dataChangeEvent.tableId());
        output.collect(
                new StreamRecord<>(
                        new PartitioningEvent(
                                dataChangeEvent,
                                hashFunction.apply(dataChangeEvent) % downstreamParallelism)));
    }

    private void broadcastEvent(Event toBroadcast) {
        for (int i = 0; i < downstreamParallelism; i++) {
            output.collect(new StreamRecord<>(new PartitioningEvent(toBroadcast, i)));
        }
    }

    private void recreateHashFunction(TableId tableId) throws Exception {
        Optional<Schema> optionalSchema = schemaEvolutionClient.getLatestSchema(tableId);
        if (!optionalSchema.isPresent()) {
            throw new IllegalStateException(
                    String.format("Unable to get latest schema for table %s", tableId));
        }
        cachedHashFunctions.put(tableId, new HashFunction(optionalSchema.get()));
    }

    private static class HashFunction implements Function<DataChangeEvent, Integer> {
        private final List<RecordData.FieldGetter> primaryKeyGetters;

        public HashFunction(Schema schema) {
            primaryKeyGetters = createFieldGetters(schema);
        }

        @Override
        public Integer apply(DataChangeEvent event) {
            List<Object> objectsToHash = new ArrayList<>();
            // Table ID
            TableId tableId = event.tableId();
            Optional.ofNullable(tableId.getNamespace()).ifPresent(objectsToHash::add);
            Optional.ofNullable(tableId.getSchemaName()).ifPresent(objectsToHash::add);
            objectsToHash.add(tableId.getTableName());

            // Primary key
            RecordData data =
                    event.op().equals(OperationType.DELETE) ? event.before() : event.after();
            for (RecordData.FieldGetter primaryKeyGetter : primaryKeyGetters) {
                objectsToHash.add(primaryKeyGetter.getFieldOrNull(data));
            }

            // Calculate hash
            return Objects.hash(objectsToHash.toArray());
        }

        private List<RecordData.FieldGetter> createFieldGetters(Schema schema) {
            List<RecordData.FieldGetter> fieldGetters =
                    new ArrayList<>(schema.primaryKeys().size());
            int[] primaryKeyPositions =
                    schema.primaryKeys().stream()
                            .mapToInt(
                                    pk -> {
                                        int i = 0;
                                        while (!schema.getColumns().get(i).getName().equals(pk)) {
                                            ++i;
                                        }
                                        if (i >= schema.getColumnCount()) {
                                            throw new IllegalStateException(
                                                    String.format(
                                                            "Unable to find column \"%s\" which is defined as primary key",
                                                            pk));
                                        }
                                        return i;
                                    })
                            .toArray();
            for (int primaryKeyPosition : primaryKeyPositions) {
                fieldGetters.add(
                        RecordData.createFieldGetter(
                                schema.getColumns().get(primaryKeyPosition).getType(),
                                primaryKeyPosition));
            }
            return fieldGetters;
        }
    }
}
