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

import org.apache.calcite.sql.SqlSelect;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/** Unit tests for the {@link FlinkSqlParser}. */
public class FlinkSqlParserTest {

    @Test
    public void testCalciteParser() {
        SqlSelect parse =
                FlinkSqlParser.parseSelect(
                        "select CONCAT(id, order_id) as uniq_id, * from tb where uniq_id > 10 and id is not null");
        Assert.assertEquals(
                "`CONCAT`(`id`, `order_id`) AS `uniq_id`, *", parse.getSelectList().toString());
        Assert.assertEquals("`uniq_id` > 10 AND `id` IS NOT NULL", parse.getWhere().toString());
    }

    @Test
    public void testParseComputedColumnNames() {
        List<String> computedColumnNames =
                FlinkSqlParser.parseComputedColumnNames("CONCAT(id, order_id) as uniq_id, *");
        Assert.assertEquals(new String[] {"uniq_id"}, computedColumnNames.toArray());
    }

    @Test
    public void testParseFilterColumnNameList() {
        List<String> computedColumnNames =
                FlinkSqlParser.parseFilterColumnNameList(" uniq_id > 10 and id is not null");
        Assert.assertEquals(new String[] {"uniq_id", "id"}, computedColumnNames.toArray());
    }

    @Test
    public void testTranslateFilterToJaninoExpression() {
        testFilterExpression("id is not null", "null != id");
        testFilterExpression("id is null", "null == id");
        testFilterExpression("id = 1 and uid = 2", "id == 1 && uid == 2");
        testFilterExpression("id = 1 or id = 2", "id == 1 || id == 2");
        testFilterExpression("id = '1'", "String.valueOf('1').equals(id)");
        testFilterExpression("id <> '1'", "!String.valueOf('1').equals(id)");
        testFilterExpression("upper(id)", "upper(id)");
        testFilterExpression("concat(a,b)", "concat(a, b)");
        testFilterExpression("id + 2", "id + 2");
        testFilterExpression("id - 2", "id - 2");
        testFilterExpression("id * 2", "id * 2");
        testFilterExpression("id / 2", "id / 2");
        testFilterExpression("id % 2", "id % 2");
        testFilterExpression("a < b", "a < b");
        testFilterExpression("a <= b", "a <= b");
        testFilterExpression("a > b", "a > b");
        testFilterExpression("a >= b", "a >= b");
        testFilterExpression("upper(lower(id))", "upper(lower(id))");
        testFilterExpression(
                "abs(uniq_id) > 10 and id is not null", "abs(uniq_id) > 10 && null != id");
    }

    private void testFilterExpression(String expression, String expressionExpect) {
        String janinoExpression =
                FlinkSqlParser.translateFilterExpressionToJaninoExpression(expression);
        Assert.assertEquals(expressionExpect, janinoExpression);
    }
}
