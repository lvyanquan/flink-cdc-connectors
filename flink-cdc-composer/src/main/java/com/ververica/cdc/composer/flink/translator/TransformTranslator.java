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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.composer.definition.TransformDef;
import com.ververica.cdc.runtime.operators.transform.FilterFunction;
import com.ververica.cdc.runtime.operators.transform.ProjectionFunction;
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

        ProjectionFunction.Builder projectionFunctionBuilder = ProjectionFunction.newBuilder();
        for (TransformDef transform : transforms) {
            projectionFunctionBuilder.addProjection(
                transform.getSourceTable(), transform.getProjection(), transform.getFilter().get());
        }
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = input.map(projectionFunctionBuilder.build(), new EventTypeInfo()).name("Transform:Projection");

        FilterFunction.Builder filterFunctionBuilder = FilterFunction.newBuilder();
        for (TransformDef transform : transforms) {
            filterFunctionBuilder.addFilter(
                transform.getSourceTable(), transform.getProjection(), transform.getFilter().get());
        }
        return singleOutputStreamOperator.filter(filterFunctionBuilder.build()).name("Transform:Filter");
    }
}
