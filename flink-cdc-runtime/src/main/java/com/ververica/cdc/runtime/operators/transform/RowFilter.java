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

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.runtime.parser.FlinkSqlParser;

/**
 * RowFilter
 */
public class RowFilter {
    private final Set<String> columnNames;
    private final JexlExpression expression;
    private static final Engine jexlEngine = new Engine();
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
        JexlExpression expression = jexlEngine.createExpression(filterExpression);
        return of(columnNames, expression);
    }

    public boolean run(BinaryRecordData after, List<Column> columns) {
        JexlContext jexlContext = new MapContext();
        for (int i = 0; i < columns.size(); i++) {
            jexlContext.set(columns.get(i).getName(), fromDataType(after.getString(i), columns.get(i).getType()));
        }
        return (Boolean) expression.evaluate(jexlContext);
    }

    private Object fromDataType(Object value, DataType dataType){
        if(value == null){
            return value;
        }
        switch (dataType.getTypeRoot()){
            case CHAR:
            case VARCHAR:
                return value.toString();
            case DECIMAL:
                return BigDecimal.valueOf((long) value);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return Integer.parseInt(value.toString());
            case FLOAT:
                return Float.parseFloat(value.toString());
            case DOUBLE:
                return Double.parseDouble(value.toString());
            case BOOLEAN:
                return Boolean.parseBoolean(value.toString());
            default:
                return value;
        }
    }
}
