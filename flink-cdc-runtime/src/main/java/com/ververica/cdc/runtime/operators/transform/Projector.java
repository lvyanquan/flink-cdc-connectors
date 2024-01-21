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

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.common.utils.StringUtils;
import com.ververica.cdc.runtime.parser.FlinkSqlParser;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import com.ververica.cdc.runtime.typeutils.DataTypeConverter;

import java.util.ArrayList;
import java.util.List;

/** The Projector applies to describe the projection of filtering tables. */
public class Projector {
    private String projection;
    private final int includeAllSourceColumnIndex;
    private final List<ColumnTransform> columnTransformList;
    private List<Column> allColumnList;
    private BinaryRecordDataGenerator recordDataGenerator;

    public Projector(
            String projection,
            int includeAllSourceColumnIndex,
            List<ColumnTransform> columnTransformList,
            BinaryRecordDataGenerator recordDataGenerator) {
        this.projection = projection;
        this.includeAllSourceColumnIndex = includeAllSourceColumnIndex;
        this.columnTransformList = columnTransformList;
        this.recordDataGenerator = recordDataGenerator;
        this.allColumnList = getAllColumnList(new ArrayList<>());
    }

    public BinaryRecordDataGenerator getRecordDataGenerator() {
        return recordDataGenerator;
    }

    public boolean isVaild() {
        // (projection.length() == 1 && includeAllSourceColumnIndex == 0): only star. Only star is
        // invalid.
        return !StringUtils.isNullOrWhitespaceOnly(projection)
                && !(projection.length() == 1 && includeAllSourceColumnIndex == 0);
    }

    private static Projector of(
            String projection,
            int includeAllSourceColumnIndex,
            List<ColumnTransform> columnTransformList,
            BinaryRecordDataGenerator recordDataGenerator) {
        return new Projector(
                projection, includeAllSourceColumnIndex, columnTransformList, recordDataGenerator);
    }

    private static RowType toRowType(List<Column> columnList) {
        DataType[] dataTypes = columnList.stream().map(Column::getType).toArray(DataType[]::new);
        String[] columnNames = columnList.stream().map(Column::getName).toArray(String[]::new);
        return RowType.of(dataTypes, columnNames);
    }

    public static Projector generateProjector(String projection) {
        if (StringUtils.isNullOrWhitespaceOnly(projection)) {
            return null;
        }
        List<ColumnTransform> columnTransformList =
                FlinkSqlParser.generateColumnTransforms(projection);
        int includeAllSourceColumnIndex = -1;
        // convert columnTransform named `*` into the flag of includeAllSourceColumn
        for (int i = 0; i < columnTransformList.size(); i++) {
            // the column name of star is ""
            if (columnTransformList.get(i).getColumnName().equals("")) {
                includeAllSourceColumnIndex = i;
                columnTransformList.remove(includeAllSourceColumnIndex);
                break;
            }
        }
        return of(projection, includeAllSourceColumnIndex, columnTransformList, null);
    }

    private boolean includeAllSourceColumn() {
        return includeAllSourceColumnIndex > -1;
    }

    private List<Column> getAllColumnList(List<Column> columns) {
        List<Column> allColumnList = new ArrayList<>();
        columnTransformList.forEach(
                columnTransform -> {
                    allColumnList.add(columnTransform.getColumn());
                });
        List<Column> sourceColumnList = new ArrayList<>();
        columns.forEach(
                column -> {
                    boolean isDuplicate = false;
                    for (ColumnTransform columnTransform : columnTransformList) {
                        if (columnTransform.getColumnName().equals(column.getName())) {
                            isDuplicate = true;
                            break;
                        }
                    }
                    if (!isDuplicate) {
                        sourceColumnList.add(column);
                    }
                });
        if (includeAllSourceColumn()) {
            allColumnList.addAll(includeAllSourceColumnIndex, sourceColumnList);
        }
        return allColumnList;
    }

    public CreateTableEvent applyCreateTableEvent(CreateTableEvent createTableEvent) {
        applyNewSchema(createTableEvent.getSchema());
        // add the column of projection into Schema
        Schema schema = createTableEvent.getSchema().copy(allColumnList);
        return new CreateTableEvent(createTableEvent.tableId(), schema);
    }

    public void applyNewSchema(Schema schema) {
        allColumnList = getAllColumnList(schema.getColumns());
        recordDataGenerator = new BinaryRecordDataGenerator(toRowType(allColumnList));
    }

    public BinaryRecordData recordData(BinaryRecordData after, TableInfo tableInfo) {
        List<Object> valueList = new ArrayList<>();
        for (Column column : allColumnList) {
            boolean isColumnTransform = false;
            for (ColumnTransform columnTransform : columnTransformList) {
                if (column.getName().equals(columnTransform.getColumnName())
                        && columnTransform.isValidProjection()) {
                    valueList.add(
                            DataTypeConverter.convert(
                                    columnTransform.evaluate(after, tableInfo),
                                    columnTransform.getDataType()));
                    isColumnTransform = true;
                    break;
                }
            }
            if (!isColumnTransform) {
                valueList.add(getValueFromBinaryRecordData(column.getName(), after, tableInfo));
            }
        }
        return getRecordDataGenerator().generate(valueList.toArray(new Object[valueList.size()]));
    }

    private Object getValueFromBinaryRecordData(
            String columnName, BinaryRecordData binaryRecordData, TableInfo tableInfo) {
        List<Column> columns = tableInfo.getSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columnName.equals(columns.get(i).getName())) {
                RecordData.FieldGetter[] fieldGetters = tableInfo.getFieldGetters();
                return DataTypeConverter.convert(
                        fieldGetters[i].getFieldOrNull(binaryRecordData), columns.get(i).getType());
            }
        }
        return null;
    }
}
