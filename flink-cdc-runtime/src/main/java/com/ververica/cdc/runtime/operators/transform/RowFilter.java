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

import org.apache.flink.table.runtime.generated.CompileUtils;

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.utils.StringUtils;
import com.ververica.cdc.runtime.parser.FlinkSqlParser;
import com.ververica.cdc.runtime.parser.JaninoParser;
import com.ververica.cdc.runtime.typeutils.DataTypeConverter;
import org.codehaus.janino.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/** The RowFilter applies to describe the row filter of filtering tables. */
public class RowFilter {
    private static final Logger LOG = LoggerFactory.getLogger(RowFilter.class);
    private final String expression;
    private final String scriptExpression;
    private final List<String> columnNames;
    private ExpressionEvaluator expressionEvaluator;

    public RowFilter(String expression, String scriptExpression, List<String> columnNames) {
        this.expression = expression;
        this.scriptExpression = scriptExpression;
        this.columnNames = columnNames;
    }

    public static RowFilter of(
            String expression, String scriptExpression, List<String> columnNames) {
        return new RowFilter(expression, scriptExpression, columnNames);
    }

    public static RowFilter generateRowFilter(String filterExpression) {
        if (StringUtils.isNullOrWhitespaceOnly(filterExpression)) {
            return null;
        }
        List<String> columnNames = FlinkSqlParser.parseFilterColumnNameList(filterExpression);
        String scriptExpression =
                FlinkSqlParser.translateFilterExpressionToJaninoExpression(filterExpression);
        return of(filterExpression, scriptExpression, columnNames);
    }

    public boolean run(BinaryRecordData after, TableInfo tableInfo) {
        List<Column> columns = tableInfo.getSchema().getColumns();
        List<Object> params = new ArrayList<>();
        List<Class<?>> paramTypes = new ArrayList<>();
        RecordData.FieldGetter[] fieldGetters = tableInfo.getFieldGetters();
        for (int i = 0; i < columns.size(); i++) {
            if (columnNames.contains(columns.get(i).getName())) {
                params.add(
                        DataTypeConverter.convertToOriginal(
                                fieldGetters[i].getFieldOrNull(after), columns.get(i).getType()));
                paramTypes.add(DataTypeConverter.convertOriginalClass(columns.get(i).getType()));
            }
        }
        if (expressionEvaluator == null) {
            expressionEvaluator =
                    CompileUtils.compileExpression(
                            JaninoParser.loadSystemFunction(scriptExpression),
                            columnNames,
                            paramTypes,
                            Boolean.class);
        }
        try {
            return (Boolean) expressionEvaluator.evaluate(params.toArray());
        } catch (InvocationTargetException e) {
            LOG.error("Table:{} filter:{} execute failed. {}", tableInfo.getName(), expression, e);
            throw new RuntimeException(e);
        }
    }

    public boolean isVaild() {
        return !columnNames.isEmpty();
    }
}
