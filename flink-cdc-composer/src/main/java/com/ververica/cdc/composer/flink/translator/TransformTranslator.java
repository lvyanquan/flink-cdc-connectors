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
package com.ververica.cdc.composer.flink.translator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.ververica.cdc.common.event.ColumnId;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.composer.definition.TransformDef;
import com.ververica.cdc.runtime.operators.transform.TransformFunction;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;

import java.util.List;

/**
 * TransformTranslator
 */
public class TransformTranslator {

    public DataStream<Event> translate(DataStream<Event> input, List<TransformDef> transforms) {
        if (transforms.isEmpty()) {
            return input;
        }
        TransformFunction.Builder transformFunctionBuilder = TransformFunction.newBuilder();
        for (TransformDef transform : transforms) {
            transform
                    .getAddColumn()
                    .forEach(
                            (key, value) -> {
                                transformFunctionBuilder.addTransform(
                                        value,
                                        ColumnId.parse(
                                                TableId.parse(transform.getSinkTable()), key));
                            });
        }
        return input.map(transformFunctionBuilder.build(), new EventTypeInfo()).name("Transform");
    }
}
