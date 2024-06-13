/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.runtime.partitioning.EventPartitioner;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEventKeySelector;
import org.apache.flink.cdc.runtime.partitioning.PostPartitionProcessor;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.cdc.runtime.typeutils.PartitioningEventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.paimon.options.Options;

/**
 * Translator used to build {@link PrePartitionOperator}, {@link EventPartitioner} and {@link
 * PostPartitionProcessor} which are responsible for events partition.
 */
@Internal
public class PartitioningTranslator {

    public DataStream<Event> translate(DataStream<Event> input, Options catalogOptions) {
        return input.transform(
                        "PrePartition",
                        new PartitioningEventTypeInfo(),
                        new PrePartitionOperator(input.getParallelism(), catalogOptions))
                .partitionCustom(new EventPartitioner(), new PartitioningEventKeySelector())
                .map(new PostPartitionProcessor(), new EventTypeInfo())
                .name("PostPartition");
    }
}
