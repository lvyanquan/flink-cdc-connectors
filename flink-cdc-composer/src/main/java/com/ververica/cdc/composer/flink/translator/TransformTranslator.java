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

import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.composer.definition.TransformDef;
import com.ververica.cdc.runtime.operators.transform.FilterFunction;
import com.ververica.cdc.runtime.operators.transform.ProjectionFunction;
import com.ververica.cdc.runtime.parser.FlinkSqlParser;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;

import java.util.List;
import java.util.Set;

/** Translator for transform. */
public class TransformTranslator {

    public DataStream<Event> translate(DataStream<Event> input, List<TransformDef> transforms) {
        if (transforms.isEmpty()) {
            return input;
        }

        ProjectionFunction.Builder projectionFunctionBuilder = ProjectionFunction.newBuilder();
        FilterFunction.Builder filterFunctionBuilder = FilterFunction.newBuilder();
        boolean containProjection = false;
        boolean containFilter = false;
        for (TransformDef transform : transforms) {
            if (transform.isValidProjection()) {
                containProjection = true;
                projectionFunctionBuilder.addProjection(
                        transform.getSourceTable(), transform.getProjection().get());
            }
            if (transform.isValidFilter()) {
                containFilter = true;
                filterFunctionBuilder.addFilter(
                        transform.getSourceTable(), transform.getFilter().get());
            }
        }

        if (containProjection && !containFilter) {
            // Only contain projection.
            return input.map(projectionFunctionBuilder.build(), new EventTypeInfo())
                    .name("Transform:Projection");
        } else if (!containProjection && containFilter) {
            // Only contain filter.
            return input.filter(filterFunctionBuilder.build()).name("Transform:Filter");
        } else {
            // Contain projection and filter.
            if (containFilteredComputedColumn(transforms)) {
                DataStream<Event> projectionOutput =
                        input.map(projectionFunctionBuilder.build(), new EventTypeInfo())
                                .name("Transform:Projection");
                return projectionOutput
                        .filter(filterFunctionBuilder.build())
                        .name("Transform:Filter");
            } else {
                DataStream<Event> filterOutput =
                        input.filter(filterFunctionBuilder.build()).name("Transform:Filter");
                return filterOutput
                        .map(projectionFunctionBuilder.build())
                        .name("Transform:Projection");
            }
        }
    }

    private boolean containFilteredComputedColumn(List<TransformDef> transforms) {
        boolean contain = false;
        for (TransformDef transformDef : transforms) {
            if (!transformDef.isValidProjection() || !transformDef.isValidProjection()) {
                continue;
            }
            Set<String> computedColumnNames =
                    FlinkSqlParser.parseComputedColumnNames(transformDef.getProjection().get());
            Set<String> filteredColumnNames =
                    FlinkSqlParser.parseColumnNames(transformDef.getFilter().get());
            for (String computedColumnName : computedColumnNames) {
                if (filteredColumnNames.contains(computedColumnName)) {
                    return true;
                }
            }
        }
        return contain;
    }
}
