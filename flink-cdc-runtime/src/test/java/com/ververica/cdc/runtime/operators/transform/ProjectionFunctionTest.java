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

import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.ColumnId;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.OperationType;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.Ignore;
import org.junit.jupiter.api.Test;

import static com.ververica.cdc.common.testutils.assertions.EventAssertions.assertThat;

import org.apache.flink.configuration.Configuration;

/**
 * TransformFunctionTest
 */
public class ProjectionFunctionTest {
    private static final TableId CUSTOMERS_TABLEID =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("phone", DataTypes.BIGINT())
                    .primaryKey("id")
                    .build();
    private static final ColumnId CUSTOMERS_COLUMNID = ColumnId.parse(CUSTOMERS_TABLEID, "c_id");
    private static final Schema EXPECT_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("phone", DataTypes.BIGINT())
                    .physicalColumn("c_id", DataTypes.INT())
                    .primaryKey("id")
                    .build();

    @Ignore
    @Test
    void testDataChangeEventTransform() throws Exception {
        ProjectionFunction transform =
                ProjectionFunction.newBuilder().addProjection("","*,id + 10 as c_id", "").build();
                transform.open(new Configuration());
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {1, new BinaryStringData("Alice"), 12345678L}));
        assertThat(transform.map(insertEvent))
                .asDataChangeEvent()
                .hasTableId(CUSTOMERS_TABLEID)
                .hasOperationType(OperationType.INSERT)
                .withAfterRecordData()
                .hasArity(4)
                .withSchema(EXPECT_SCHEMA)
                .hasFields(11, new BinaryStringData("Alice"), 12345678L, 111);
    }
}
