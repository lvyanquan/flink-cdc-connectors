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
import org.apache.commons.jexl3.MapContext;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.parser.FlinkSqlParser;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;

/**
 * Projector
 */
public class Projector {
    private String projection;
    private final int includeAllSourceColumnIndex;
    private final List<ColumnTransform> columnTransformList;

    private BinaryRecordDataGenerator recordDataGenerator;

    public Projector(String projection, int includeAllSourceColumnIndex, List<ColumnTransform> columnTransformList, BinaryRecordDataGenerator recordDataGenerator) {
        this.projection = projection;
        this.includeAllSourceColumnIndex = includeAllSourceColumnIndex;
        this.columnTransformList = columnTransformList;
        this.recordDataGenerator = recordDataGenerator;
    }

    public List<ColumnTransform> getColumnTransformList() {
        return columnTransformList;
    }

    public BinaryRecordDataGenerator getRecordDataGenerator() {
        return recordDataGenerator;
    }

    private static Projector of(String projection, int includeAllSourceColumnIndex, List<ColumnTransform> columnTransformList, BinaryRecordDataGenerator recordDataGenerator) {
        return new Projector(projection, includeAllSourceColumnIndex, columnTransformList, recordDataGenerator);
    }

    private static RowType toRowType(List<ColumnTransform> columnTransformList){
        DataType[] dataTypes = columnTransformList.stream().map(ColumnTransform::getDataType).toArray(DataType[]::new);
        String[] columnNames = columnTransformList.stream().map(ColumnTransform::getColumnName).toArray(String[]::new);
        return RowType.of(dataTypes, columnNames);
    }

    private static List<Column> toColumnList(List<ColumnTransform> columnTransformList){
        return columnTransformList.stream().map(columnTransform -> {
            return Column.physicalColumn(columnTransform.getColumnName(), columnTransform.getDataType());
        }).collect(Collectors.toList());
    }

    public static Projector generateProjector(String projection) {
        SqlSelect sqlSelect = FlinkSqlParser.parseProjection(projection);
        List<ColumnTransform> columnTransformList = FlinkSqlParser.generateColumnTransforms(sqlSelect.getSelectList());
        int includeAllSourceColumnIndex = -1;
        // convert columnTransform named `*` into the flag of includeAllSourceColumn
        for(int i=0;i< columnTransformList.size();i++){
            // the column name of star is ""
            if(columnTransformList.get(i).getColumnName().equals("")){
                includeAllSourceColumnIndex = i;
                columnTransformList.remove(i);
                break;
            }
        }
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(toRowType(columnTransformList));
        return of(projection, includeAllSourceColumnIndex, columnTransformList, generator);
    }

    private boolean includeAllSourceColumn(){
        return includeAllSourceColumnIndex > -1;
    }

    public void applyProjector(SchemaChangeEvent schemaChangeEvent) {
        if(!includeAllSourceColumn()){
            return;
        }
        if(schemaChangeEvent instanceof CreateTableEvent){
            CreateTableEvent createTableEvent = (CreateTableEvent) schemaChangeEvent;
            List<Column> sourceColumns = createTableEvent.getSchema().getColumns();
            List<ColumnTransform> sourceColumnTransform = new ArrayList<>();
            sourceColumns.forEach(sourceColumn->{
                sourceColumnTransform.add(ColumnTransform.of(sourceColumn.getName(), sourceColumn.getType()));
            });
            columnTransformList.addAll(includeAllSourceColumnIndex, sourceColumnTransform);
            recordDataGenerator = new BinaryRecordDataGenerator(toRowType(columnTransformList));
            // add the column of projection into Schema
            createTableEvent.getSchema().copy(toColumnList(columnTransformList));
        }
    }

    public BinaryRecordData generateRecordData(BinaryRecordData after, List<Column> columns) {
        List<Object> valueList = new ArrayList<>();
        Map<String, Object> originalValueMap = new ConcurrentHashMap<>();
        JexlContext jexlContext = new MapContext();
        for (int i = 0; i < columns.size(); i++) {
            originalValueMap.put(columns.get(i).getName(), fromDataType(after.getString(i), columns.get(i).getType()));
            jexlContext.set(columns.get(i).getName(), fromDataType(after.getString(i), columns.get(i).getType()));
        }

        for (ColumnTransform columnTransform : columnTransformList) {
            if (originalValueMap.containsKey(columnTransform.getColumnName())) {
                valueList.add(toDataType(originalValueMap.get(columnTransform.getColumnName()).toString(), columnTransform.getDataType()));
            } else {
                valueList.add(toDataType(columnTransform.evaluate(jexlContext), columnTransform.getDataType()));
            }
        }
        return getRecordDataGenerator().generate(valueList.toArray(new Object[valueList.size()]));
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

    private Object toDataType(Object value, DataType dataType){
        if(value == null){
            return BinaryStringData.fromString("");
        }
        switch (dataType.getTypeRoot()){
            case CHAR:
            case VARCHAR:
                return BinaryStringData.fromString(value.toString());
            case DECIMAL:
                BigDecimal bigDecimalValue = (BigDecimal) value;
                return DecimalData.fromBigDecimal(bigDecimalValue, bigDecimalValue.precision(), bigDecimalValue.scale());
            default:
                return BinaryStringData.fromString(value.toString());
        }
    }
}
