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


import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.internal.Engine;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Selectors;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** A map function that applies user-defined transform logics. */
public class FilterFunction extends RichFilterFunction<Event> {
    private final List<Tuple3<String, String, String>> filterRules;
    private transient List<Tuple2<Selectors, JexlExpression>> filters;
    private transient Engine jexlEngine;
    private transient List<DataType> dataTypes;
    private transient List<String> columnNames;

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
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jexlEngine = new Engine();
        dataTypes = new ArrayList<>();
        columnNames = new ArrayList<>();
        filters =
            filterRules.stream()
                .map(
                    tuple3 -> {
                        String tableInclusions = tuple3.f0;
                        String filterExpression = tuple3.f2;
                        JexlExpression expression = jexlEngine.createExpression(filterExpression);
                        Selectors selectors =
                            new Selectors.SelectorsBuilder()
                                .includeTables(tableInclusions)
                                .build();
                        return new Tuple2<>(selectors, expression);
                    })
                .collect(Collectors.toList());
        // todo: Change to retrieve from metadata
        columnNames.add("col1");
        columnNames.add("col2");
        columnNames.add("col12");
        dataTypes.add(DataTypes.STRING());
        dataTypes.add(DataTypes.STRING());
        dataTypes.add(DataTypes.STRING());

    }

    @Override
    public boolean filter(Event event) throws Exception {
        if (!(event instanceof DataChangeEvent)) {
            return true;
        }
        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        TableId tableId = dataChangeEvent.tableId();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if(after == null){
            return false;
        }

        JexlContext jexlContext = new MapContext();

        for(int i = 0; i<after.getArity();i++){
            // todo: Convert type
            jexlContext.set(columnNames.get(i), after.getString(i).toString());
        }

        for (Tuple2<Selectors, JexlExpression> route : filters) {
            Selectors selectors = route.f0;
            if (selectors.isMatch(tableId)) {
                JexlExpression expression = route.f1;
                return (Boolean) expression.evaluate(jexlContext);
            }
        }

        return true;
    }
}
