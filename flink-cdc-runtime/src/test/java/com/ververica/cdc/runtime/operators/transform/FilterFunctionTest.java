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

import static com.ververica.cdc.common.testutils.assertions.EventAssertions.assertThat;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;

/**
 * FilterFunctionTest
 */
public class FilterFunctionTest {
    private static final TableId CUSTOMERS_TABLEID =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("col2", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();
    private static final Schema EXPECT_SCHEMA =
            Schema.newBuilder()
                .physicalColumn("col1", DataTypes.STRING())
                .physicalColumn("col2", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    void testDataChangeEventTransformProjection() throws Exception {
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
        FilterFunction transform =
            FilterFunction.newBuilder().addFilter(CUSTOMERS_TABLEID.identifier(),"col1, col2","col1 < 2").build();
                transform.open(new Configuration());
        transform.filter(createTableEvent);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), new BinaryStringData("1")}));
        DataChangeEvent insertEvent2 =
            DataChangeEvent.insertEvent(
                CUSTOMERS_TABLEID,
                recordDataGenerator.generate(
                    new Object[] {new BinaryStringData("2"), new BinaryStringData("2")}));
        assertThat(transform.filter(insertEvent)).isTrue();
        assertThat(transform.filter(insertEvent2)).isFalse();
    }
}
