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

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.testutils.MySqlVersion;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import com.ververica.cdc.debezium.table.DebeziumChangelogMode;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.fetchResults;
import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.getServerId;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for MySQL event source. */
public class MySqlFullTypesITCase extends MySqlSourceTestBase {

    private static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-gtids/expire-seconds/my.cnf");

    private static final BinaryRecordDataGenerator COMMON_TYPES_GENERATOR =
            new BinaryRecordDataGenerator(
                    RowType.of(
                            DataTypes.DECIMAL(20, 0).notNull(),
                            DataTypes.TINYINT(),
                            DataTypes.SMALLINT(),
                            DataTypes.SMALLINT(),
                            DataTypes.SMALLINT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.DECIMAL(20, 0),
                            DataTypes.DECIMAL(20, 0),
                            DataTypes.VARCHAR(255),
                            DataTypes.CHAR(3),
                            DataTypes.DOUBLE(), // fixme: REAL cannot be converted to DOUBLE
                            DataTypes.FLOAT(),
                            DataTypes.FLOAT(),
                            DataTypes.FLOAT(),
                            DataTypes.DOUBLE(),
                            DataTypes.DOUBLE(),
                            DataTypes.DOUBLE(),
                            DataTypes.DECIMAL(8, 4),
                            DataTypes.DECIMAL(8, 4),
                            DataTypes.DECIMAL(8, 4),
                            DataTypes.DECIMAL(6, 0),
                            DataTypes.DECIMAL(20, 1),
                            DataTypes.BOOLEAN(),
                            DataTypes.BOOLEAN(),
                            DataTypes.BOOLEAN()));

    private final UniqueDatabase fullTypesMySql57Database =
            new UniqueDatabase(MYSQL_CONTAINER, "column_type_test", TEST_USER, TEST_PASSWORD);
    private final UniqueDatabase fullTypesMySql8Database =
            new UniqueDatabase(
                    MYSQL8_CONTAINER, "column_type_test_mysql8", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeClass
    public static void beforeClass() {
        LOG.info("Starting MySql8 containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        LOG.info("Container MySql8 is started.");
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("Stopping MySql8 containers...");
        MYSQL8_CONTAINER.stop();
        LOG.info("Container MySql8 is stopped.");
    }

    @Before
    public void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    public void testMysql57CommonDataTypes() throws Throwable {
        testCommonDataTypes(fullTypesMySql57Database);
    }

    @Test
    public void testMySql8CommonDataTypes() throws Throwable {
        testCommonDataTypes(fullTypesMySql8Database);
    }

    public void testCommonDataTypes(UniqueDatabase database) throws Exception {
        database.createAndInitialize();
        MySqlSourceConfig sourceConfig = getConfig(new String[] {"common_types"}, database);
        TableId tableId = TableId.tableId(database.getDatabaseName(), "common_types");

        CloseableIterator<Event> iterator =
                env.fromSource(
                                getMySqlSource(sourceConfig),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect();

        List<Event> snapshotResults = fetchResults(iterator, 1);

        List<Event> expected =
                Collections.singletonList(
                        DataChangeEvent.insertEvent(
                                tableId,
                                COMMON_TYPES_GENERATOR.generate(
                                        new Object[] {
                                            DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                                            (byte) 127,
                                            (short) 255,
                                            (short) 255,
                                            (short) 32767,
                                            65535,
                                            65535,
                                            8388607,
                                            16777215,
                                            16777215,
                                            2147483647,
                                            4294967295L,
                                            4294967295L,
                                            2147483647L,
                                            9223372036854775807L,
                                            DecimalData.fromBigDecimal(
                                                    new BigDecimal("18446744073709551615"), 20, 0),
                                            DecimalData.fromBigDecimal(
                                                    new BigDecimal("18446744073709551615"), 20, 0),
                                            BinaryStringData.fromString("Hello World"),
                                            BinaryStringData.fromString("abc"),
                                            (double) 123.102f, // fixme: REAL converted to DOUBLE
                                            123.102f,
                                            123.103f,
                                            123.104f,
                                            404.4443d,
                                            404.4444d,
                                            404.4445d,
                                            DecimalData.fromBigDecimal(
                                                    new BigDecimal("1234.5678"), 8, 4),
                                            DecimalData.fromBigDecimal(
                                                    new BigDecimal("123.4568"), 8, 4),
                                            DecimalData.fromBigDecimal(
                                                    new BigDecimal("123.4569"), 8, 4),
                                            DecimalData.fromBigDecimal(
                                                    new BigDecimal("345.6"), 6, 0),
                                            DecimalData.fromBigDecimal(
                                                    new BigDecimal("34567892.1"), 65, 1),
                                            false,
                                            true,
                                            true
                                        })));

        assertThat(snapshotResults).isEqualTo(expected);
    }

    private MySqlSource<Event> getMySqlSource(MySqlSourceConfig sourceConfig) {
        MySqlEventDeserializer deserializer =
                new MySqlEventDeserializer(
                        DebeziumChangelogMode.ALL,
                        ZoneId.of(sourceConfig.getServerTimeZone()),
                        sourceConfig.isIncludeSchemaChanges());

        return MySqlSource.<Event>builder()
                .hostname(sourceConfig.getHostname())
                .port(sourceConfig.getPort())
                .databaseList(sourceConfig.getDatabaseList().toArray(new String[0]))
                .tableList(sourceConfig.getTableList().toArray(new String[0]))
                .username(sourceConfig.getUsername())
                .password(sourceConfig.getPassword())
                .deserializer(deserializer)
                .serverId(getServerId(env.getParallelism()))
                .serverTimeZone(sourceConfig.getServerTimeZone())
                .debeziumProperties(sourceConfig.getDbzProperties())
                .build();
    }

    private MySqlSourceConfig getConfig(String[] captureTables, UniqueDatabase database) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> database.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.latest())
                .databaseList(database.getDatabaseName())
                .tableList(captureTableIds)
                .includeSchemaChanges(false)
                .hostname(database.getHost())
                .port(database.getDatabasePort())
                .splitSize(10)
                .fetchSize(2)
                .username(database.getUsername())
                .password(database.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .createConfig(0);
    }
}
