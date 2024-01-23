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

package com.ververica.cdc.runtime.parser.validate;

import org.apache.flink.calcite.shaded.org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.apache.flink.table.api.TableException;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.planner.plan.type.FlinkReturnTypes.ARG0_VARCHAR_FORCE_NULLABLE;

/** FlinkCDCOperatorTable to generate the metadata of calcite. */
public class FlinkCDCOperatorTable extends ReflectiveSqlOperatorTable {

    private static @MonotonicNonNull FlinkCDCOperatorTable instance;

    /*private static final SqlOperatorTable stdOperatorTable = SqlStdOperatorTable.instance();
    private final ListMultimap<String, SqlOperator> opMap = ArrayListMultimap.create();
    private final List<SqlOperator> operators = new ArrayList<>();*/

    private FlinkCDCOperatorTable() {}

    public static synchronized FlinkCDCOperatorTable instance() {
        if (instance == null) {
            instance = new FlinkCDCOperatorTable();
            instance.init();
            // ensure no dynamic function declares directly
            validateNoDynamicFunction(instance);

            // register functions based on batch or streaming mode
            final FlinkCDCOperatorTable finalInstance = instance;
            dynamicFunctions().forEach(f -> finalInstance.register(f));
        }

        return instance;
    }

    public static List<SqlFunction> dynamicFunctions() {
        return Arrays.asList();
    }

    private static void validateNoDynamicFunction(FlinkCDCOperatorTable instance)
            throws TableException {
        instance.getOperatorList()
                .forEach(
                        op -> {
                            if (op.isDynamicFunction() && op.isDeterministic()) {
                                throw new TableException(
                                        String.format(
                                                "Dynamic function: %s is not allowed declaring directly, please add it to initializing.",
                                                op.getName()));
                            }
                        });
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier opName,
            @Nullable SqlFunctionCategory sqlFunctionCategory,
            SqlSyntax syntax,
            List<SqlOperator> operatorList,
            SqlNameMatcher nameMatcher) {
        /*//首先查找Calcite原生的函数列表
        stdOperatorTable.lookupOperatorOverloads(opName, sqlFunctionCategory, syntax, operatorList, SqlNameMatchers.withCaseSensitive(false));

        //如果Calcite中没有找到，则在我们自己的函数列表中查找
        if (operatorList.isEmpty() && syntax == SqlSyntax.FUNCTION && opName.isSimple()) {
            List<SqlOperator> ops = opMap.get(opName.getSimple().toUpperCase(Locale.US));
            if (ops != null) {
                operatorList.addAll(ops);
            }
        }*/
        // set caseSensitive=false to make sure the behavior is same with before.
        super.lookupOperatorOverloads(
                opName,
                sqlFunctionCategory,
                syntax,
                operatorList,
                SqlNameMatchers.withCaseSensitive(false));
    }

    /*@Override
    public List<SqlOperator> getOperatorList() {
        return operators;
    }*/

    /*public void registerFunction()
    {
        String functionName = "SUBSTR";
        boolean isDeterministic = true;
        boolean isDynamic = false;
        //生成函数的入参
        //Presto SUBSTR 函数有多种实现，这里只注册了 substr(string, start) → varchar 这种实现
        List<SqlTypeFamily> argumentTypes = ImmutableList.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC);
        //用于函数入参匹配和检查
        SqlOperandTypeChecker checker = OperandTypes.family(argumentTypes);
        //函数返回值
        SqlReturnTypeInference returnTypeInference = ReturnTypes.CHAR;
        org.apache.calcite.sql.SqlFunction sqlFunction = new FlinkCDCSqlOperatorImpl(functionName,
            isDeterministic,
            isDynamic,
            returnTypeInference,
            SqlSyntax.FUNCTION,
            checker);
        opMap.put(functionName, sqlFunction);
        operators.add(sqlFunction);
    }*/

    public static final SqlFunction SUBSTR2 =
            new SqlFunction(
                    "SUBSTR2",
                    SqlKind.OTHER_FUNCTION,
                    ARG0_VARCHAR_FORCE_NULLABLE,
                    null,
                    OperandTypes.or(
                            OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
                            OperandTypes.family(
                                    SqlTypeFamily.CHARACTER,
                                    SqlTypeFamily.INTEGER,
                                    SqlTypeFamily.INTEGER)),
                    SqlFunctionCategory.STRING);
}
