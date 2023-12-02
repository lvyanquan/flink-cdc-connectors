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

package com.ververica.cdc.common.pipeline;

import com.ververica.cdc.common.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static com.ververica.cdc.common.pipeline.PipelineOptions.GLOBAL_PARALLELISM;
import static com.ververica.cdc.common.pipeline.PipelineOptions.PIPELINE_LOCAL_TIME_ZONE;
import static com.ververica.cdc.common.pipeline.PipelineOptions.PIPELINE_NAME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for {@link PipelineConfig}. */
class PipelineConfigTest {

    @Test
    void testAddAndGetConfiguration() {
        final PipelineConfig pipelineConfig = new PipelineConfig();
        final Configuration configuration = new Configuration();
        configuration.set(PIPELINE_NAME, "test_pipeline_job");
        configuration.set(GLOBAL_PARALLELISM, 128);
        pipelineConfig.addConfiguration(configuration);

        assertThat(pipelineConfig.get(PIPELINE_NAME)).isEqualTo("test_pipeline_job");
        assertThat(pipelineConfig.get(GLOBAL_PARALLELISM)).isEqualTo(128);
        assertThat(pipelineConfig.getConfiguration().toMap().entrySet().size()).isEqualTo(2);
    }

    @Test
    void testSetAndGetConfigOption() {
        final PipelineConfig pipelineConfig = new PipelineConfig();
        pipelineConfig.set(PIPELINE_NAME, "test_pipeline_job");
        assertThat(pipelineConfig.get(PIPELINE_NAME)).isEqualTo("test_pipeline_job");
        assertThat(pipelineConfig.getOptional(GLOBAL_PARALLELISM)).isEmpty();
    }

    @Test
    void testSetAndGetLocalTimeZone() {
        final PipelineConfig pipelineConfig = new PipelineConfig();
        assertThat(pipelineConfig.get(PIPELINE_LOCAL_TIME_ZONE))
                .isEqualTo(PIPELINE_LOCAL_TIME_ZONE.defaultValue());
        assertThat(pipelineConfig.getLocalTimeZone().toString())
                .isNotEqualTo(PIPELINE_LOCAL_TIME_ZONE.defaultValue());
        pipelineConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        assertThat(pipelineConfig.getLocalTimeZone()).isEqualTo(ZoneId.of("Asia/Shanghai"));

        assertThatThrownBy(
                        () -> {
                            pipelineConfig.set(PIPELINE_LOCAL_TIME_ZONE, "invalid time zone");
                            pipelineConfig.getLocalTimeZone();
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Invalid time zone. The valid value should be a Time Zone Database ID"
                                + " such as 'America/Los_Angeles' to include daylight saving time. "
                                + "Fixed offsets are supported using 'GMT-08:00' or 'GMT+08:00'. "
                                + "Or use 'UTC' without time zone and daylight saving time.");
    }
}
