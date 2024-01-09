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
import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Selectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** A map function that applies user-defined transform logics. */
public class FilterFunction extends RichFilterFunction<Event> {
    private final List<Tuple2<String, String>> filterRules;
    private transient List<Tuple2<Selectors, RowFilter>> filters;
    private final Map<TableId, List<Column>> columnMap;

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder of {@link FilterFunction}. */
    public static class Builder {
        private final List<Tuple2<String, String>> filterRules = new ArrayList<>();

        public Builder addFilter(String tableInclusions, String filter) {
            filterRules.add(Tuple2.of(tableInclusions, filter));
            return this;
        }

        public FilterFunction build() {
            return new FilterFunction(filterRules);
        }
    }

    private FilterFunction(List<Tuple2<String, String>> filterRules) {
        this.filterRules = filterRules;
        this.columnMap = new ConcurrentHashMap<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        filters =
                filterRules.stream()
                        .map(
                                tuple2 -> {
                                    String tableInclusions = tuple2.f0;
                                    String filterExpression = tuple2.f1;
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
            cacheSchema((CreateTableEvent) event);
            return true;
        }
        if (!(event instanceof DataChangeEvent)) {
            return true;
        }
        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        TableId tableId = dataChangeEvent.tableId();
        for (Tuple2<Selectors, RowFilter> route : filters) {
            Selectors selectors = route.f0;
            if (selectors.isMatch(tableId)) {
                RowFilter rowFilter = route.f1;
                BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
                BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
                if (after != null) {
                    return rowFilter.run(after, columnMap.get(tableId));
                } else if (before != null) {
                    return rowFilter.run(before, columnMap.get(tableId));
                }
            }
        }

        return true;
    }

    private void cacheSchema(CreateTableEvent createTableEvent) {
        columnMap.put(createTableEvent.tableId(), createTableEvent.getSchema().getColumns());
    }
}
