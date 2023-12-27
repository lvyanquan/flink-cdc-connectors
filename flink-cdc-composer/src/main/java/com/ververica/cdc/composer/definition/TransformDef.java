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

import java.util.Map;
import java.util.Objects;

/**
 * Definition of transformation.
 *
 * <p>Transformation will be implemented later, therefore we left the class blank.
 */
public class TransformDef {
    private final String sinkTable;
    private final Map<String, String> addColumn;

    public TransformDef(String sinkTable, Map<String, String> addColumn) {
        this.sinkTable = sinkTable;
        this.addColumn = addColumn;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public Map<String, String> getAddColumn() {
        return addColumn;
    }

    @Override
    public String toString() {
        return "TransformDef{"
                + "sinkTable='"
                + sinkTable
                + '\''
                + ", addColumn="
                + addColumn
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
        TransformDef that = (TransformDef) o;
        return Objects.equals(sinkTable, that.sinkTable)
                && Objects.equals(addColumn, that.addColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sinkTable, addColumn);
    }
}
