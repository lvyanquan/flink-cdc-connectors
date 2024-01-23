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

package com.ververica.cdc.runtime.parser.validate;

import org.apache.flink.calcite.shaded.com.google.common.collect.Multimap;

import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** FlinkCDCSchema to generate the metadata of calcite. */
public class FlinkCDCSchema extends AbstractSchema {

    private String name;
    private List<FlinkCDCTable> tables;
    private Multimap<String, Function> functions;

    public FlinkCDCSchema(
            String name, List<FlinkCDCTable> tables, Multimap<String, Function> functions) {
        this.name = name;
        this.tables = tables;
        this.functions = functions;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tables.stream().collect(Collectors.toMap(FlinkCDCTable::getName, t -> t));
    }

    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        return functions;
    }
}
