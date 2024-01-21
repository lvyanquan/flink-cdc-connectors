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
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;

import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.utils.StringUtils;
import com.ververica.cdc.runtime.operators.transform.ColumnTransform;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.planner.utils.TableConfigUtils.getCalciteConfig;

/** Use Flink's calcite parser to parse the statement of flink cdc pipeline transform. */
public class FlinkSqlParser {

    private static final CalciteParser calciteParser = getCalciteParser();

    private static CalciteParser getCalciteParser() {
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setSqlDialect(SqlDialect.DEFAULT);
        CalciteConfig calciteConfig = getCalciteConfig(tableConfig);
        SqlParser.Config sqlParserConfig =
                JavaScalaConversionUtil.<SqlParser.Config>toJava(calciteConfig.getSqlParserConfig())
                        .orElseGet(
                                // we use Java lex because back ticks are easier than double quotes
                                // in
                                // programming and cases are preserved
                                () -> {
                                    SqlConformance conformance = FlinkSqlConformance.DEFAULT;
                                    return SqlParser.config()
                                            .withParserFactory(
                                                    FlinkSqlParserFactories.create(conformance))
                                            .withConformance(conformance)
                                            .withLex(Lex.JAVA)
                                            .withIdentifierMaxLength(256);
                                });
        return new CalciteParser(sqlParserConfig);
    }

    public static SqlSelect parseSelect(String statement) {
        SqlNode sqlNode = calciteParser.parse(statement);
        if (sqlNode instanceof SqlSelect) {
            return (SqlSelect) sqlNode;
        } else {
            throw new ParseException("Only select statements can be parsed.");
        }
    }

    // Parse all columns
    public static List<ColumnTransform> generateColumnTransforms(String projectionExpression) {
        List<ColumnTransform> columnTransformList = new ArrayList<>();
        if (StringUtils.isNullOrWhitespaceOnly(projectionExpression)) {
            return columnTransformList;
        }
        SqlSelect sqlSelect = parseProjectionExpression(projectionExpression);
        if (sqlSelect.getSelectList().isEmpty()) {
            return columnTransformList;
        }
        for (SqlNode sqlNode : sqlSelect.getSelectList()) {
            if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                if (SqlKind.AS.equals(sqlBasicCall.getOperator().kind)) {
                    SqlBasicCall transform = null;
                    String columnName = null;
                    List<SqlNode> operandList = sqlBasicCall.getOperandList();
                    for (SqlNode operand : operandList) {
                        if (operand instanceof SqlBasicCall) {
                            transform = (SqlBasicCall) operand;
                        } else if (operand instanceof SqlIdentifier) {
                            columnName = ((SqlIdentifier) operand).getSimple();
                        }
                    }
                    for (ColumnTransform columnTransform : columnTransformList) {
                        if (columnTransform.getColumnName().equals(columnName)) {
                            throw new ParseException("Duplicate column definitions: " + columnName);
                        }
                    }
                    columnTransformList.add(
                            ColumnTransform.of(
                                    columnName,
                                    DataTypes.STRING(),
                                    JaninoParser.translateSqlNodeToJaninoExpression(transform),
                                    parseColumnNameList(transform)));
                } else {
                    throw new ParseException("Unrecognized projection: " + sqlBasicCall.toString());
                }
            } else if (sqlNode instanceof SqlIdentifier) {
                SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                columnTransformList.add(
                        ColumnTransform.of(sqlIdentifier.getSimple(), DataTypes.STRING()));
            } else {
                throw new ParseException("Unrecognized projection: " + sqlNode.toString());
            }
        }
        return columnTransformList;
    }

    public static String translateFilterExpressionToJaninoExpression(String filterExpression) {
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
        return JaninoParser.translateSqlNodeToJaninoExpression((SqlBasicCall) where);
    }

    public static List<String> parseComputedColumnNames(String projection) {
        List<String> columnNames = new ArrayList<>();
        if (StringUtils.isNullOrWhitespaceOnly(projection)) {
            return columnNames;
        }
        SqlSelect sqlSelect = parseProjectionExpression(projection);
        if (sqlSelect.getSelectList().isEmpty()) {
            return columnNames;
        }
        for (SqlNode sqlNode : sqlSelect.getSelectList()) {
            if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                if (SqlKind.AS.equals(sqlBasicCall.getOperator().kind)) {
                    String columnName = null;
                    List<SqlNode> operandList = sqlBasicCall.getOperandList();
                    for (SqlNode operand : operandList) {
                        if (operand instanceof SqlIdentifier) {
                            columnName = ((SqlIdentifier) operand).getSimple();
                        }
                    }
                    if (columnNames.contains(columnName)) {
                        throw new ParseException("Duplicate column definitions: " + columnName);
                    }
                    columnNames.add(columnName);
                } else {
                    throw new ParseException("Unrecognized projection: " + sqlBasicCall.toString());
                }
            } else if (sqlNode instanceof SqlIdentifier) {
                continue;
            } else {
                throw new ParseException("Unrecognized projection: " + sqlNode.toString());
            }
        }
        return columnNames;
    }

    public static List<String> parseFilterColumnNameList(String filterExpression) {
        if (StringUtils.isNullOrWhitespaceOnly(filterExpression)) {
            return new ArrayList<>();
        }
        SqlSelect sqlSelect = parseFilterExpression(filterExpression);
        if (!sqlSelect.hasWhere()) {
            return new ArrayList<>();
        }
        SqlNode where = sqlSelect.getWhere();
        if (!(where instanceof SqlBasicCall)) {
            throw new ParseException("Unrecognized where: " + where.toString());
        }
        SqlBasicCall sqlBasicCall = (SqlBasicCall) where;
        return parseColumnNameList(sqlBasicCall);
    }

    private static List<String> parseColumnNameList(SqlBasicCall sqlBasicCall) {
        List<String> columnNameList = new ArrayList<>();
        findSqlIdentifier(sqlBasicCall.getOperandList(), columnNameList);
        return columnNameList;
    }

    private static void findSqlIdentifier(List<SqlNode> sqlNodes, List<String> columnNameList) {
        for (SqlNode sqlNode : sqlNodes) {
            if (sqlNode instanceof SqlIdentifier) {
                columnNameList.add(((SqlIdentifier) sqlNode).getSimple());
            } else if (sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                findSqlIdentifier(sqlBasicCall.getOperandList(), columnNameList);
            }
        }
    }

    private static SqlSelect parseProjectionExpression(String projection) {
        StringBuilder statement = new StringBuilder();
        statement.append("SELECT ");
        statement.append(projection);
        statement.append(" FROM TB");
        return parseSelect(statement.toString());
    }

    public static SqlSelect parseFilterExpression(String filterExpression) {
        StringBuilder statement = new StringBuilder();
        statement.append("SELECT * FROM TB");
        if (!StringUtils.isNullOrWhitespaceOnly(filterExpression)) {
            statement.append(" WHERE ");
            statement.append(filterExpression);
        }
        return parseSelect(statement.toString());
    }
}
