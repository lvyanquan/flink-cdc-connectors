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

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.utils.Preconditions;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;

import static com.ververica.cdc.common.pipeline.PipelineOptions.PIPELINE_LOCAL_TIME_ZONE;

/** Configuration for the current pipeline to adjust composer process. */
@PublicEvolving
public class PipelineConfig {

    public PipelineConfig() {}

    /*
     * A configuration object to hold all configuration that has been set specifically in the pipeline.
     */
    private final Configuration configuration = new Configuration();

    /**
     * Adds the given key-value configuration to the underlying application-specific configuration.
     * Note this will overwrite existing keys.
     *
     * @param configuration key-value configuration to be added
     */
    public void addConfiguration(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        this.configuration.addAll(configuration);
    }

    /**
     * Gives direct access to the underlying application-specific key-value map for advanced
     * configuration.
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /** Sets an application-specific value for the given {@link ConfigOption}. */
    public <T> PipelineConfig set(ConfigOption<T> option, T value) {
        configuration.set(option, value);
        return this;
    }

    /** Gets an application-specific value for the given {@link ConfigOption}. */
    public <T> T get(ConfigOption<T> option) {
        return configuration.get(option);
    }

    /** Gets an optional application-specific value for the given {@link ConfigOption}. */
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        return configuration.getOptional(option);
    }

    /**
     * Returns the current session time zone id. It is used when converting to/from {@code TIMESTAMP
     * WITH LOCAL TIME ZONE}. See {@link #setLocalTimeZone(ZoneId)} for more details.
     *
     * @see LocalZonedTimestampType
     */
    public ZoneId getLocalTimeZone() {
        final String zone = configuration.get(PIPELINE_LOCAL_TIME_ZONE);
        if (PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(zone)) {
            return ZoneId.systemDefault();
        }
        validateTimeZone(zone);
        return ZoneId.of(zone);
    }

    /**
     * Sets the current session time zone id. It is used when converting to/from {@code TIMESTAMP
     * WITH LOCAL TIME ZONE}. Internally, timestamps with local time zone are always represented in
     * the UTC time zone. However, when converting to data types that don't include a time zone
     * (e.g. TIMESTAMP, STRING), the session time zone is used during conversion.
     *
     * @see LocalZonedTimestampType
     */
    public void setLocalTimeZone(ZoneId zoneId) {
        final String zone;
        if (zoneId instanceof ZoneOffset) {
            // Give ZoneOffset a timezone for backwards compatibility reasons.
            zone = ZoneId.ofOffset("GMT", (ZoneOffset) zoneId).toString();
        } else {
            zone = zoneId.toString();
        }
        validateTimeZone(zone);

        configuration.set(PIPELINE_LOCAL_TIME_ZONE, zone);
    }

    private static void validateTimeZone(String zone) {
        boolean isValid;
        try {
            isValid = TimeZone.getTimeZone(zone).toZoneId().equals(ZoneId.of(zone));
        } catch (Exception var3) {
            isValid = false;
        }

        if (!isValid) {
            throw new IllegalArgumentException(
                    "Invalid time zone. The valid value should be a Time Zone Database ID "
                            + "such as 'America/Los_Angeles' to include daylight saving time. "
                            + "Fixed offsets are supported using 'GMT-08:00' or 'GMT+08:00'. "
                            + "Or use 'UTC' without time zone and daylight saving time.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PipelineConfig)) {
            return false;
        }
        PipelineConfig that = (PipelineConfig) o;
        return Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration);
    }

    // --------------------------------------------------------------------------------------------

    /** Creates a new PipelineConfig that is initialized with the given configuration. */
    public static PipelineConfig fromConfiguration(Configuration configuration) {
        final PipelineConfig pipelineConfig = new PipelineConfig();
        pipelineConfig.addConfiguration(configuration);
        return pipelineConfig;
    }
}
