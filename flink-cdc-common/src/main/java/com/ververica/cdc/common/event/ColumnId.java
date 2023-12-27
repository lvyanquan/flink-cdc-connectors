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
package com.ververica.cdc.common.event;

import com.ververica.cdc.common.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * ColumnId
 *
 * @author qiwenkai
 * @since 2023/12/26 14:06
 */
@PublicEvolving
public class ColumnId implements Serializable {
    private final TableId tableId;
    private final String columnName;

    public ColumnId(TableId tableId, String columnName) {
        this.tableId = tableId;
        this.columnName = columnName;
    }

    public TableId getTableId() {
        return tableId;
    }

    public String getColumnName() {
        return columnName;
    }

    public static ColumnId parse(TableId tableId, String columnName) {
        return new ColumnId(tableId, columnName);
    }
}
