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

import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.OperationType;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.testutils.assertions.DataChangeEventAssert;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.ververica.cdc.common.testutils.assertions.EventAssertions.assertThat;

/** Unit tests for the {@link ProjectionFunction}. */
public class ProjectionFunctionTest {
    private static final TableId CUSTOMERS_TABLEID =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("col2", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();
    private static final Schema CUSTOMERS_SOURCE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("col2", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();
    private static final Schema EXPECT_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("col2", DataTypes.STRING())
                    .physicalColumn("col12", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();

    private static final TableId DATATYPE_TABLEID =
            TableId.tableId("my_company", "my_branch", "data_types");
    private static final Schema DATATYPE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("colString", DataTypes.STRING())
                    .physicalColumn("colBoolean", DataTypes.BOOLEAN())
                    .physicalColumn("colTinyint", DataTypes.TINYINT())
                    .physicalColumn("colSmallint", DataTypes.SMALLINT())
                    .physicalColumn("colInt", DataTypes.INT())
                    .physicalColumn("colBigint", DataTypes.BIGINT())
                    .physicalColumn("colDate", DataTypes.DATE())
                    .physicalColumn("colTime", DataTypes.TIME())
                    .physicalColumn("colTimestamp", DataTypes.TIMESTAMP())
                    .physicalColumn("colFloat", DataTypes.FLOAT())
                    .physicalColumn("colDouble", DataTypes.DOUBLE())
                    .physicalColumn("colDecimal", DataTypes.DECIMAL(6, 2))
                    .primaryKey("colString")
                    .build();

    @Test
    void testDataChangeEventTransformProjection() throws Exception {
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
        ProjectionFunction transform =
                ProjectionFunction.newBuilder()
                        .addProjection(CUSTOMERS_TABLEID.identifier(), "*, col1 + col2 col12")
                        .build();
        transform.open(new Configuration());

        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SOURCE_SCHEMA.toRowDataType()));

        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("2")
                                }));
        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("2")
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("3")
                                }));
        assertThat(transform.map(createTableEvent))
                .asSchemaChangeEvent()
                .asCreateTableEvent()
                .hasTableId(CUSTOMERS_TABLEID)
                .hasSchema(EXPECT_SCHEMA);
        assertThat(transform.map(insertEvent))
                .asDataChangeEvent()
                .hasTableId(CUSTOMERS_TABLEID)
                .hasOperationType(OperationType.INSERT)
                .withAfterRecordData()
                .hasArity(3)
                .withSchema(EXPECT_SCHEMA)
                .hasFields(
                        new BinaryStringData("1"),
                        new BinaryStringData("2"),
                        new BinaryStringData("12"));
        DataChangeEventAssert updateEventAssert =
                assertThat(transform.map(updateEvent))
                        .asDataChangeEvent()
                        .hasTableId(CUSTOMERS_TABLEID)
                        .hasOperationType(OperationType.UPDATE);
        updateEventAssert
                .withBeforeRecordData()
                .hasArity(3)
                .withSchema(EXPECT_SCHEMA)
                .hasFields(
                        new BinaryStringData("1"),
                        new BinaryStringData("2"),
                        new BinaryStringData("12"));
        updateEventAssert
                .withAfterRecordData()
                .hasArity(3)
                .withSchema(EXPECT_SCHEMA)
                .hasFields(
                        new BinaryStringData("1"),
                        new BinaryStringData("3"),
                        new BinaryStringData("13"));
    }

    @Test
    void testDataChangeEventTransformProjectionDataTypeConvert() throws Exception {
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(DATATYPE_TABLEID, DATATYPE_SCHEMA);
        ProjectionFunction transform =
                ProjectionFunction.newBuilder()
                        .addProjection(DATATYPE_TABLEID.identifier(), "*")
                        .build();
        transform.open(new Configuration());

        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) DATATYPE_SCHEMA.toRowDataType()));

        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        DATATYPE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("3.14"),
                                    new Boolean(true),
                                    new Byte("1"),
                                    new Short("1"),
                                    new Integer(1),
                                    new Long(1),
                                    new Integer(1704471599),
                                    new Integer(1704471599),
                                    TimestampData.fromMillis(1704471599),
                                    new Float(3.14f),
                                    new Double(3.14d),
                                    DecimalData.fromBigDecimal(new BigDecimal(3.14), 6, 2),
                                }));
        assertThat(transform.map(createTableEvent))
                .asSchemaChangeEvent()
                .asCreateTableEvent()
                .hasTableId(DATATYPE_TABLEID)
                .hasSchema(DATATYPE_SCHEMA);
        assertThat(transform.map(insertEvent))
                .asDataChangeEvent()
                .hasTableId(DATATYPE_TABLEID)
                .hasOperationType(OperationType.INSERT)
                .withAfterRecordData()
                .hasArity(12)
                .withSchema(DATATYPE_SCHEMA)
                .hasFields(
                        new BinaryStringData("3.14"),
                        new Boolean(true),
                        new Byte("1"),
                        new Short("1"),
                        new Integer(1),
                        new Long(1),
                        new Integer(1704471599),
                        new Integer(1704471599),
                        TimestampData.fromMillis(1704471599),
                        new Float(3.14f),
                        new Double(3.14d),
                        DecimalData.fromBigDecimal(new BigDecimal(3.14), 6, 2));
    }
}
