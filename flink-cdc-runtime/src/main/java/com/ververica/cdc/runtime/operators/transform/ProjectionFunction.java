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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** A map function that applies user-defined transform logics. */
public class ProjectionFunction extends RichMapFunction<Event, Event> {
    private final List<Tuple3<String, String, String>> projectionRules;
    private transient List<Tuple2<JexlExpression, String>> projection;
    private transient Engine jexlEngine;
    private transient List<DataType> dataTypes;
    private transient List<String> columnNames;

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder of {@link ProjectionFunction}. */
    public static class Builder {
        private final List<Tuple3<String, String, String>> projectionRules = new ArrayList<>();

        public Builder addProjection(String tableInclusions, String projection, String filter) {
            projectionRules.add(Tuple3.of(tableInclusions, projection, filter));
            return this;
        }

        public ProjectionFunction build() {
            return new ProjectionFunction(projectionRules);
        }
    }

    private ProjectionFunction(List<Tuple3<String, String, String>> projectionRules) {
        this.projectionRules = projectionRules;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jexlEngine = new Engine();
        dataTypes = new ArrayList<>();
        columnNames = new ArrayList<>();
        projection =
            projectionRules.stream()
                .map(
                    tuple3 -> {
                        String tableInclusions = tuple3.f0;
                        // todo: parse expression
//                        String projectionExpression = tuple3.f1;
                        String projectionExpression = "col1 + col2";
                        JexlExpression expression = jexlEngine.createExpression(projectionExpression);
                        return new Tuple2<>(expression, tableInclusions);
                    })
                .collect(Collectors.toList());
        // todo: Change to retrieve from metadata
        columnNames.add("col1");
        columnNames.add("col2");
        columnNames.add("col12");
        dataTypes.add(DataTypes.STRING());
        dataTypes.add(DataTypes.STRING());
        dataTypes.add(DataTypes.STRING());

        /*for (Tuple3<String, String, String> route : projectionRules) {
            ColumnId addBy = route.f1;
            columnNames.add(addBy.getColumnName());
            dataTypes.add(DataTypes.STRING());
        }*/
    }

    @Override
    public Event map(Event event) throws Exception {
        if (!(event instanceof DataChangeEvent)) {
            return event;
        }
        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if(after == null){
            return event;
        }
        List<Object> valueList = new ArrayList<>();

        JexlContext jexlContext = new MapContext();

        for(int i=0;i<after.getArity();i++){
            valueList.add(BinaryStringData.fromString(after.getString(i).toString()));
        }

        for(int i = 0; i<after.getArity();i++){
            // todo: Convert type
            jexlContext.set(columnNames.get(i), after.getString(i).toString());
        }

        for (Tuple2<JexlExpression, String> route : projection) {
            JexlExpression expression = route.f0;
            Object evaluate = expression.evaluate(jexlContext);
            valueList.add(BinaryStringData.fromString(evaluate.toString()));
        }

        RowType rowType =
                RowType.of(
                        dataTypes.toArray(new DataType[dataTypes.size()]),
                        columnNames.toArray(new String[columnNames.size()]));
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        BinaryRecordData data =
                generator.generate(
                    valueList.toArray(new Object[columnNames.size()])
                );

        return DataChangeEvent.setAfter(dataChangeEvent, data);
    }
}
