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

package com.ververica.cdc.runtime.parser;

import org.apache.flink.api.common.io.ParseException;

import com.ververica.cdc.common.utils.StringUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

/** Use Janino parser to parse the statement of flink cdc pipeline transform. */
public class JaninoParser {

    public static String translateFilterToJaninoExpression(String filterExpression) {
        if (StringUtils.isNullOrWhitespaceOnly(filterExpression)) {
            return "";
        }
        SqlSelect sqlSelect = FlinkSqlParser.parseFilterExpression(filterExpression);
        if (!sqlSelect.hasWhere()) {
            return "";
        }
        SqlNode where = sqlSelect.getWhere();
        if (!(where instanceof SqlBasicCall)) {
            throw new ParseException("Unrecognized where: " + where.toString());
        }
        Java.Rvalue rvalue = translateJaninoAST((SqlBasicCall) where);
        return rvalue.toString();
    }

    private static Java.Rvalue translateJaninoAST(SqlBasicCall sqlBasicCall) {
        List<SqlNode> operandList = sqlBasicCall.getOperandList();
        List<Java.Rvalue> atoms = new ArrayList<>();
        for (SqlNode sqlNode : operandList) {
            if (sqlNode instanceof SqlIdentifier) {
                SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                atoms.add(
                        new Java.AmbiguousName(
                                Location.NOWHERE, new String[] {sqlIdentifier.getSimple()}));
            } else if (sqlNode instanceof SqlLiteral) {
                SqlLiteral sqlLiteral = (SqlLiteral) sqlNode;
                String value = sqlLiteral.getValue().toString();
                if (sqlLiteral instanceof SqlCharStringLiteral) {
                    value = "String.valueOf(" + value + ")";
                }
                atoms.add(new Java.AmbiguousName(Location.NOWHERE, new String[] {value}));
            } else if (sqlNode instanceof SqlBasicCall) {
                atoms.add(translateJaninoAST((SqlBasicCall) sqlNode));
            }
        }
        return sqlBasicCallToJaninoRvalue(sqlBasicCall, atoms.toArray(new Java.Rvalue[0]));
    }

    private static Java.Rvalue sqlBasicCallToJaninoRvalue(
            SqlBasicCall sqlBasicCall, Java.Rvalue[] atoms) {
        switch (sqlBasicCall.getKind()) {
            case AND:
                return new Java.BinaryOperation(Location.NOWHERE, atoms[0], "&&", atoms[1]);
            case EQUALS:
                if (atoms.length != 2) {
                    throw new ParseException("Unrecognized filter: " + sqlBasicCall.toString());
                }
                if (atoms[0] instanceof Java.AmbiguousName) {
                    if (atoms[0].toString().contains("String.valueOf(")) {
                        return new Java.MethodInvocation(
                                Location.NOWHERE, atoms[0], "equals", new Java.Rvalue[] {atoms[1]});
                    } else if (atoms[1].toString().contains("String.valueOf(")) {
                        return new Java.MethodInvocation(
                                Location.NOWHERE, atoms[1], "equals", new Java.Rvalue[] {atoms[0]});
                    }
                }
                return new Java.BinaryOperation(Location.NOWHERE, atoms[0], "==", atoms[1]);
            case IS_NOT_NULL:
                return new Java.UnaryOperation(Location.NOWHERE, "null != ", atoms[0]);
            case OTHER_FUNCTION:
                return new Java.MethodInvocation(
                        Location.NOWHERE, null, sqlBasicCall.getOperator().getName(), atoms);
            default:
                String operation = sqlBasicCall.getKind().sql;
                if (atoms.length > 1) {
                    return new Java.BinaryOperation(
                            Location.NOWHERE, atoms[0], operation, atoms[1]);
                } else if (atoms.length == 1) {
                    return new Java.UnaryOperation(Location.NOWHERE, operation, atoms[0]);
                } else {
                    throw new ParseException("Unrecognized filter: " + sqlBasicCall.toString());
                }
        }
    }
}
