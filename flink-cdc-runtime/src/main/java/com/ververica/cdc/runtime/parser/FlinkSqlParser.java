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
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;

import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.utils.StringUtils;
import com.ververica.cdc.runtime.operators.transform.ColumnTransform;
import com.ververica.cdc.runtime.parser.validate.FlinkCDCOperatorTable;
import com.ververica.cdc.runtime.parser.validate.FlinkCDCSchemaFactory;
import com.ververica.cdc.runtime.typeutils.DataTypeConverter;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.TableConfigUtils.getCalciteConfig;

/** Use Flink's calcite parser to parse the statement of flink cdc pipeline transform. */
public class FlinkSqlParser {

    private static final CalciteParser calciteParser = getCalciteParser();
    private static final String DEFAULT_SCHEMA = "default_schema";
    private static final String DEFAULT_TABLE = "TB";

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

    private static RelNode sqlToRel(List<Column> columns, SqlNode sqlNode) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        Map<String, Object> operand = new HashMap<>();
        operand.put("tableName", DEFAULT_TABLE);
        operand.put("columns", columns);
        rootSchema.add(
                DEFAULT_SCHEMA,
                FlinkCDCSchemaFactory.INSTANCE.create(rootSchema.plus(), DEFAULT_SCHEMA, operand));
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader calciteCatalogReader =
                new CalciteCatalogReader(
                        rootSchema,
                        rootSchema.path(DEFAULT_SCHEMA),
                        factory,
                        new CalciteConnectionConfigImpl(new Properties()));
        FlinkCDCOperatorTable flinkCDCOperatorTable = FlinkCDCOperatorTable.instance();
        FlinkSqlOperatorTable flinkSqlOperatorTable = FlinkSqlOperatorTable.instance(false);
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        SqlOperatorTables.chain(flinkSqlOperatorTable, flinkCDCOperatorTable),
                        calciteCatalogReader,
                        factory,
                        SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
        SqlNode validateSqlNode = validator.validate(sqlNode);
        SqlToRelConverter sqlToRelConverter =
                new SqlToRelConverter(
                        null,
                        validator,
                        calciteCatalogReader,
                        RelOptCluster.create(
                                new HepPlanner(new HepProgramBuilder().build()),
                                new RexBuilder(factory)),
                        StandardConvertletTable.INSTANCE,
                        SqlToRelConverter.config().withTrimUnusedFields(false));
        RelRoot relRoot = sqlToRelConverter.convertQuery(validateSqlNode, false, true);
        return relRoot.rel;
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
    public static List<ColumnTransform> generateColumnTransforms(
            String projectionExpression, List<Column> columns) {
        if (StringUtils.isNullOrWhitespaceOnly(projectionExpression)) {
            return new ArrayList<>();
        }
        SqlSelect sqlSelect = parseProjectionExpression(projectionExpression);
        if (sqlSelect.getSelectList().isEmpty()) {
            return new ArrayList<>();
        }
        RelNode relNode = sqlToRel(columns, sqlSelect);
        Map<String, RelDataType> relDataTypeMap =
                relNode.getRowType().getFieldList().stream()
                        .collect(
                                Collectors.toMap(
                                        RelDataTypeField::getName, RelDataTypeField::getType));
        List<ColumnTransform> columnTransformList = new ArrayList<>();
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
                            SqlIdentifier sqlIdentifier = (SqlIdentifier) operand;
                            columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                        }
                    }
                    boolean hasReplacedDuplicateColumn = false;
                    for (int i = 0; i < columnTransformList.size(); i++) {
                        if (columnTransformList.get(i).getColumnName().equals(columnName)
                                && !columnTransformList.get(i).isValidProjection()) {
                            hasReplacedDuplicateColumn = true;
                            columnTransformList.set(
                                    i,
                                    ColumnTransform.of(
                                            columnName,
                                            DataTypeConverter.convertCalciteRelDataTypeToDataType(
                                                    relDataTypeMap.get(columnName)),
                                            JaninoParser.translateSqlNodeToJaninoExpression(
                                                    transform),
                                            parseColumnNameList(transform)));
                            break;
                        }
                    }
                    if (!hasReplacedDuplicateColumn) {
                        columnTransformList.add(
                                ColumnTransform.of(
                                        columnName,
                                        DataTypeConverter.convertCalciteRelDataTypeToDataType(
                                                relDataTypeMap.get(columnName)),
                                        JaninoParser.translateSqlNodeToJaninoExpression(transform),
                                        parseColumnNameList(transform)));
                    }
                } else {
                    throw new ParseException("Unrecognized projection: " + sqlBasicCall.toString());
                }
            } else if (sqlNode instanceof SqlIdentifier) {
                SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                columnTransformList.add(
                        ColumnTransform.of(
                                columnName,
                                DataTypeConverter.convertCalciteRelDataTypeToDataType(
                                        relDataTypeMap.get(columnName))));
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
                            SqlIdentifier sqlIdentifier = (SqlIdentifier) operand;
                            columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
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
                SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
                String columnName = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
                columnNameList.add(columnName);
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
        statement.append(" FROM ");
        statement.append(DEFAULT_TABLE);
        return parseSelect(statement.toString());
    }

    public static SqlSelect parseFilterExpression(String filterExpression) {
        StringBuilder statement = new StringBuilder();
        statement.append("SELECT * FROM ");
        statement.append(DEFAULT_TABLE);
        if (!StringUtils.isNullOrWhitespaceOnly(filterExpression)) {
            statement.append(" WHERE ");
            statement.append(filterExpression);
        }
        return parseSelect(statement.toString());
    }
}
