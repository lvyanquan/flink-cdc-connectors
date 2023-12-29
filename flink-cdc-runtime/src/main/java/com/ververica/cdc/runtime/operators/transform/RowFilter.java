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

import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.internal.Engine;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.runtime.parser.FlinkSqlParser;

/**
 * RowFilter
 */
public class RowFilter {
    private final Set<String> columnNames;
    private final JexlExpression expression;

    public RowFilter(Set<String> columnNames, JexlExpression expression) {
        this.columnNames = columnNames;
        this.expression = expression;
    }

    public Set<String> getColumnNames() {
        return columnNames;
    }

    public JexlExpression getExpression() {
        return expression;
    }

    public static RowFilter of(Set<String> columnNames, JexlExpression expression) {
        return new RowFilter(columnNames, expression);
    }

    public static RowFilter generateRowFilter(String filterExpression) {
        SqlSelect sqlSelect = FlinkSqlParser.parseFilterExpression(filterExpression);
        Set<String> columnNames = FlinkSqlParser.generateColumnNames(sqlSelect.getWhere());
        Engine jexlEngine = new Engine();
        JexlExpression expression = jexlEngine.createExpression(filterExpression);
        return of(columnNames, expression);
    }

    public boolean run(BinaryRecordData after) {
        JexlContext jexlContext = new MapContext();
        Map<String, Object> originalValueMap = new ConcurrentHashMap<>();

        // todo: Obtain original values based on field names
        for(int i=0;i<after.getArity();i++){
            originalValueMap.put("col"+(i+1),BinaryStringData.fromString(after.getString(i).toString()));
        }

        for (String columnName: columnNames) {
            jexlContext.set(columnName, originalValueMap.get(columnName).toString());
        }

        return (Boolean) expression.evaluate(jexlContext);
    }
}
