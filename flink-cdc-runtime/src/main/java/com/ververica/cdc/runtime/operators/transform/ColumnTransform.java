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
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.utils.StringUtils;
import com.ververica.cdc.runtime.parser.JaninoParser;
import com.ververica.cdc.runtime.typeutils.DataTypeConverter;
import org.codehaus.janino.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/** The ColumnTransform applies to describe the information of the transformation column. */
public class ColumnTransform implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnTransform.class);
    private final Column column;
    private final String expression;
    private final String scriptExpression;
    private final List<String> originalColumnNames;

    public ColumnTransform(
            Column column,
            String expression,
            String scriptExpression,
            List<String> originalColumnNames) {
        this.column = column;
        this.expression = expression;
        this.scriptExpression = scriptExpression;
        this.originalColumnNames = originalColumnNames;
    }

    public Column getColumn() {
        return column;
    }

    public String getColumnName() {
        return column.getName();
    }

    public DataType getDataType() {
        return column.getType();
    }

    public boolean isValidProjection() {
        return !StringUtils.isNullOrWhitespaceOnly(scriptExpression);
    }

    public Object evaluate(BinaryRecordData after, TableInfo tableInfo) {
        List<Object> params = new ArrayList<>();
        List<Class<?>> paramTypes = new ArrayList<>();
        List<Column> columns = tableInfo.getSchema().getColumns();
        RecordData.FieldGetter[] fieldGetters = tableInfo.getFieldGetters();
        for (String originalColumnName : originalColumnNames) {
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                if (column.getName().equals(originalColumnName)) {
                    paramTypes.add(DataTypeConverter.convertOriginalClass(column.getType()));
                    params.add(
                            DataTypeConverter.convertToOriginal(
                                    fieldGetters[i].getFieldOrNull(after), column.getType()));
                    break;
                }
            }
        }
        ExpressionEvaluator expressionEvaluator =
                CompileUtils.compileExpression(
                        JaninoParser.loadSystemFunction(scriptExpression),
                        originalColumnNames,
                        paramTypes,
                        DataTypeConverter.convertOriginalClass(column.getType()));
        try {
            return expressionEvaluator.evaluate(params.toArray());
        } catch (InvocationTargetException e) {
            LOG.error(
                    "Table:{} column:{} projection:{} execute failed. {}",
                    tableInfo.getName(),
                    column.getName(),
                    expression,
                    e);
            throw new RuntimeException(e);
        }
    }

    public static ColumnTransform of(String columnName, DataType dataType) {
        return new ColumnTransform(Column.physicalColumn(columnName, dataType), null, null, null);
    }

    public static ColumnTransform of(
            String columnName,
            DataType dataType,
            String expression,
            String scriptExpression,
            List<String> originalColumnNames) {
        return new ColumnTransform(
                Column.physicalColumn(columnName, dataType),
                expression,
                scriptExpression,
                originalColumnNames);
    }
}
