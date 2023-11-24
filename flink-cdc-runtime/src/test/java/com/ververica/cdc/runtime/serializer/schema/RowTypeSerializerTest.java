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

package com.ververica.cdc.runtime.serializer.schema;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.serializer.SerializerTestBase;

import java.util.Arrays;
import java.util.List;

/** A test for the {@link RowTypeSerializer}. */
public class RowTypeSerializerTest extends SerializerTestBase<RowType> {
    @Override
    protected TypeSerializer<RowType> createSerializer() {
        return RowTypeSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<RowType> getTypeClass() {
        return RowType.class;
    }

    @Override
    protected RowType[] getTestData() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField("col1", DataTypes.BIGINT()),
                        new DataField("col2", DataTypes.STRING(), "desc"));
        return new RowType[] {new RowType(true, fields), new RowType(false, fields)};
    }
}
