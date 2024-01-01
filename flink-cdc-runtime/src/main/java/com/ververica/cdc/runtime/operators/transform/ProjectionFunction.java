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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Selectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** A map function that applies user-defined transform logics. */
public class ProjectionFunction extends RichMapFunction<Event, Event> {
    private final List<Tuple2<String, String>> projectionRules;
    private transient List<Tuple2<Selectors, Projector>> projection;
    private final Map<TableId, List<Column>> sourceColumnMap;

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder of {@link ProjectionFunction}. */
    public static class Builder {
        private final List<Tuple2<String, String>> projectionRules = new ArrayList<>();

        public Builder addProjection(String tableInclusions, String projection) {
            projectionRules.add(Tuple2.of(tableInclusions, projection));
            return this;
        }

        public ProjectionFunction build() {
            return new ProjectionFunction(projectionRules);
        }
    }

    private ProjectionFunction(List<Tuple2<String, String>> projectionRules) {
        this.projectionRules = projectionRules;
        this.sourceColumnMap = new ConcurrentHashMap<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        projection =
                projectionRules.stream()
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
    public Event map(Event event) throws Exception {
        if (event instanceof CreateTableEvent) {
            return transformCreateTableEvent((CreateTableEvent) event);
        }
        if (!(event instanceof DataChangeEvent)) {
            return event;
        }
        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        // skip delete event
        if (after == null) {
            return event;
        }
        TableId tableId = dataChangeEvent.tableId();

        for (Tuple2<Selectors, Projector> route : projection) {
            Selectors selectors = route.f0;
            if (selectors.isMatch(tableId)) {
                Projector projector = route.f1;
                BinaryRecordData data =
                        projector.generateRecordData(after, sourceColumnMap.get(tableId));
                return DataChangeEvent.setAfter(dataChangeEvent, data);
            }
        }

        return event;
    }

    private SchemaChangeEvent transformCreateTableEvent(CreateTableEvent createTableEvent) {
        List<Column> sourceColumn =
                new ArrayList<>(
                        Arrays.asList(
                                new Column[createTableEvent.getSchema().getColumns().size()]));
        Collections.copy(sourceColumn, createTableEvent.getSchema().getColumns());
        sourceColumnMap.put(createTableEvent.tableId(), sourceColumn);
        TableId tableId = createTableEvent.tableId();
        for (Tuple2<Selectors, Projector> route : projection) {
            Selectors selectors = route.f0;
            if (selectors.isMatch(tableId)) {
                Projector projector = route.f1;
                // update the column of projection and add the column of projection into Schema
                return projector.applyProjector(createTableEvent);
            }
        }
        return createTableEvent;
    }
}
