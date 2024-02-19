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
package com.ververica.cdc.composer.flink.executors;

import com.ververica.cdc.composer.PipelineComposeExecutor;
import org.apache.commons.cli.CommandLine;

/** Create deployment methods corresponding to different goals */
public class ComposeExecutorFactory {

    public PipelineComposeExecutor getFlinkComposeExecutor(CommandLine commandLine)
            throws Exception {
        String target = commandLine.getOptionValue("target");
        if (target.equalsIgnoreCase("kubernetes-application")) {
            return new K8SApplicationComposeExecutor();
        }
        throw new Exception(String.format("target %s is not support", target));
    }
}
