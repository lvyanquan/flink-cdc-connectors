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
package com.ververica.cdc.composer;

import org.apache.flink.configuration.Configuration;

import org.apache.commons.cli.CommandLine;

import java.nio.file.Path;
import java.util.List;

/** PipelineComposeExecutor to execute flink cdc job from different target */
public interface PipelineComposeExecutor {

    PipelineExecution.ExecutionInfo deploy(
            CommandLine commandLine, Configuration flinkConfig, List<Path> additionalJars)
            throws Exception;
}
