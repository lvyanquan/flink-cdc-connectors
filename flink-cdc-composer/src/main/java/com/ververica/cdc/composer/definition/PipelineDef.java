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

package com.ververica.cdc.composer.definition;

import com.ververica.cdc.common.pipeline.PipelineConfig;

import java.util.List;
import java.util.Objects;

/**
 * Definition of a pipeline.
 *
 * <p>A pipeline consists of following components:
 *
 * <ul>
 *   <li>Source: data source of the pipeline. Required in the definition.
 *   <li>Sink: data destination of the pipeline. Required in the definition.
 *   <li>Routes: routers specifying the connection between source tables and sink tables. Optional
 *       in the definition.
 *   <li>Transforms: transformations for applying modifications to data change events. Optional in
 *       the definition.
 *   <li>Config: configurations of the pipeline. Optional in the definition.
 * </ul>
 *
 * <p>This class keeps track of the raw pipeline definition made by users via pipeline definition
 * file. A definition will be translated to a {@link com.ververica.cdc.composer.PipelineExecution}
 * by {@link com.ververica.cdc.composer.PipelineComposer} before being submitted to the computing
 * engine.
 */
public class PipelineDef {
    private final SourceDef source;
    private final SinkDef sink;
    private final List<RouteDef> routes;
    private final List<TransformDef> transforms;
    private final PipelineConfig pipelineConfig;

    public PipelineDef(
            SourceDef source,
            SinkDef sink,
            List<RouteDef> routes,
            List<TransformDef> transforms,
            PipelineConfig pipelineConfig) {
        this.source = source;
        this.sink = sink;
        this.routes = routes;
        this.transforms = transforms;
        this.pipelineConfig = pipelineConfig;
    }

    public SourceDef getSource() {
        return source;
    }

    public SinkDef getSink() {
        return sink;
    }

    public List<RouteDef> getRoute() {
        return routes;
    }

    public List<TransformDef> getTransforms() {
        return transforms;
    }

    public PipelineConfig getPipelineConfig() {
        return pipelineConfig;
    }

    @Override
    public String toString() {
        return "PipelineDef{"
                + "source="
                + source
                + ", sink="
                + sink
                + ", routes="
                + routes
                + ", transforms="
                + transforms
                + ", pipelineConfig="
                + pipelineConfig
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PipelineDef that = (PipelineDef) o;
        return Objects.equals(source, that.source)
                && Objects.equals(sink, that.sink)
                && Objects.equals(routes, that.routes)
                && Objects.equals(transforms, that.transforms)
                && Objects.equals(pipelineConfig, that.pipelineConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, sink, routes, transforms, pipelineConfig);
    }
}
