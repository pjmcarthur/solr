/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.api;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import org.apache.solr.handler.admin.ContainerPluginsApi;

/** A source for Container Plugin configurations */
public interface ContainerPluginsSource {

  /**
   * Get the Container Plugins Read Api for this plugin source
   *
   * @return A {@link ContainerPluginsApi} Read Api for this plugin source
   */
  ContainerPluginsApi.Read getReadApi();

  /**
   * Get the Container Plugins Edit Api for this plugin source, if it supports edit operations
   *
   * @return A {@link ContainerPluginsApi} Edit Api for this plugin source, or null if the plugin
   *     source does not support editing the plugin configs
   */
  ContainerPluginsApi.Edit getEditApi();

  /** Get the Container plugin configurations from this source */
  Map<String, Object> plugins() throws IOException;

  /**
   * Persist the updated set of plugin configs
   *
   * @param modifier A function providing the map of plugin configs to be persisted
   */
  void persistPlugins(Function<Map<String, Object>, Map<String, Object>> modifier)
      throws IOException;
}
