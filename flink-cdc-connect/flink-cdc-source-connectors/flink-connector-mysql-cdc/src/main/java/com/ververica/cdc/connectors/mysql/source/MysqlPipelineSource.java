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

import org.apache.flink.connector.base.source.reader.RecordEmitter;

import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlPipelineRecordEmitter;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

/** A source for mysql pipeline connector. */
public class MysqlPipelineSource extends MySqlSource<Event> {

    MysqlPipelineSource(
            MySqlSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<Event> deserializationSchema) {
        super(configFactory, deserializationSchema);
    }

    @Override
    protected RecordEmitter<SourceRecords, Event, MySqlSplitState> createEmitter(
            MySqlSourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            MySqlSourceConfig sourceConfig) {
        return new MySqlPipelineRecordEmitter(
                deserializationSchema,
                sourceReaderMetrics,
                sourceConfig.isIncludeSchemaChanges(),
                sourceConfig);
    }
}
