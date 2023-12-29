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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
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
    private final List<ColumnTransform> columnTransformList;
    private final RowType rowType;
    private final BinaryRecordDataGenerator recordDataGenerator;

    public Projector(List<ColumnTransform> columnTransformList, RowType rowType, BinaryRecordDataGenerator recordDataGenerator) {
        this.columnTransformList = columnTransformList;
        this.rowType = rowType;
        this.recordDataGenerator = recordDataGenerator;
    }

    public List<ColumnTransform> getColumnTransformList() {
        return columnTransformList;
    }

    public RowType getRowType() {
        return rowType;
    }

    public BinaryRecordDataGenerator getRecordDataGenerator() {
        return recordDataGenerator;
    }

    private static Projector of(List<ColumnTransform> columnTransformList, RowType rowType, BinaryRecordDataGenerator recordDataGenerator) {
        return new Projector(columnTransformList, rowType, recordDataGenerator);
    }

    public static Projector generateProjector(String projection) {
        SqlSelect sqlSelect = FlinkSqlParser.parseProjection(projection);
        List<ColumnTransform> columnTransformList = FlinkSqlParser.generateColumnTransforms(sqlSelect.getSelectList());
        DataType[] dataTypes = columnTransformList.stream().map(columnTransform -> DataTypes.STRING()).toArray(DataType[]::new);
        String[] columnNames = columnTransformList.stream().map(ColumnTransform::getColumnName).toArray(String[]::new);
        RowType rowType = RowType.of(dataTypes, columnNames);
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        return of(columnTransformList, rowType, generator);
    }

    public BinaryRecordData generateRecordData(BinaryRecordData after) {
        List<Object> valueList = new ArrayList<>();
        Map<String, Object> originalValueMap = new ConcurrentHashMap<>();

        // todo: Obtain original values based on field names
        for (int i = 0; i < after.getArity(); i++) {
            originalValueMap.put("col" + (i + 1), BinaryStringData.fromString(after.getString(i).toString()));
        }

        for (DataField dataField : rowType.getFields()) {
            if (originalValueMap.containsKey(dataField.getName())) {
                valueList.add(BinaryStringData.fromString(originalValueMap.get(dataField.getName()).toString()));
            } else {
                valueList.add(BinaryStringData.fromString(originalValueMap.get("col1").toString() + originalValueMap.get("col2")));
            }
        }
        return getRecordDataGenerator().generate(valueList.toArray(new Object[valueList.size()]));
    }


}
