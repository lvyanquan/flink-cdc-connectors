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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
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
public class FilterFunction extends RichFilterFunction<Event> {
    private final List<Tuple3<String, String, String>> filterRules;
    private transient List<Tuple2<Selectors, RowFilter>> filters;
    private final Map<TableId, List<Column>> columnMap;

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder of {@link FilterFunction}. */
    public static class Builder {
        private final List<Tuple3<String, String, String>> filterRules = new ArrayList<>();

        public Builder addFilter(String tableInclusions, String projection, String filter) {
            filterRules.add(Tuple3.of(tableInclusions, projection, filter));
            return this;
        }

        public FilterFunction build() {
            return new FilterFunction(filterRules);
        }
    }

    private FilterFunction(List<Tuple3<String, String, String>> filterRules) {
        this.filterRules = filterRules;
        this.columnMap = new ConcurrentHashMap<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        filters =
                filterRules.stream()
                        .map(
                                tuple3 -> {
                                    String tableInclusions = tuple3.f0;
                                    String filterExpression = tuple3.f2;
                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple2<>(
                                            selectors,
                                            RowFilter.generateRowFilter(filterExpression));
                                })
                        .collect(Collectors.toList());
    }

    @Override
    public boolean filter(Event event) throws Exception {
        if (event instanceof CreateTableEvent) {
            transformCreateTableEvent((CreateTableEvent) event);
            return true;
        }
        if (!(event instanceof DataChangeEvent)) {
            return true;
        }
        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        TableId tableId = dataChangeEvent.tableId();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        // delete data event will be skipped
        if (after == null) {
            return true;
        }

        for (Tuple2<Selectors, RowFilter> route : filters) {
            Selectors selectors = route.f0;
            if (selectors.isMatch(tableId)) {
                RowFilter rowFilter = route.f1;
                return rowFilter.run(after, columnMap.get(tableId));
            }
        }

        return true;
    }

    private void transformCreateTableEvent(CreateTableEvent createTableEvent) {
        List<Column> sourceColumn =
                new ArrayList<>(
                        Arrays.asList(
                                new Column[createTableEvent.getSchema().getColumns().size()]));
        Collections.copy(sourceColumn, createTableEvent.getSchema().getColumns());
        columnMap.put(createTableEvent.tableId(), sourceColumn);
    }
}
