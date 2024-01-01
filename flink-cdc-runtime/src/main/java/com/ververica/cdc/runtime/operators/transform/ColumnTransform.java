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

import com.ververica.cdc.common.types.DataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.internal.Engine;

import java.io.Serializable;

/** The ColumnTransform applies to describe the information of the transformation column. */
public class ColumnTransform implements Serializable {
    private final String columnName;
    private final DataType dataType;
    private final SqlBasicCall transform;
    private final JexlExpression expression;
    private static final Engine jexlEngine = new Engine();

    public ColumnTransform(String columnName, DataType dataType, SqlBasicCall transform) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.transform = transform;
        if (transform != null) {
            this.expression = jexlEngine.createExpression(transform.toString().replace("`", ""));
        } else {
            this.expression = null;
        }
    }

    public String getColumnName() {
        return columnName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public SqlBasicCall getTransform() {
        return transform;
    }

    public JexlExpression getExpression() {
        return expression;
    }

    public Object evaluate(JexlContext jexlContext) {
        return expression.evaluate(jexlContext);
    }

    public static ColumnTransform of(String columnName, DataType dataType) {
        return new ColumnTransform(columnName, dataType, null);
    }

    public static ColumnTransform of(String columnName, DataType dataType, SqlBasicCall transform) {
        return new ColumnTransform(columnName, dataType, transform);
    }
}
