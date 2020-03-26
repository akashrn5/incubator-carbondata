/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.presto;

import java.net.URI;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;

import io.prestosql.plugin.hive.DynamicConfigurationProvider;
import io.prestosql.plugin.hive.HdfsEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class CarbonConfigurationProvider implements DynamicConfigurationProvider {

  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonConfigurationProvider.class.getName());

  @Override
  public void updateConfiguration(Configuration configuration, HdfsEnvironment.HdfsContext context,
      URI uri) {
    String queryId = context.getQueryId().orElse("queryId");

    if (!queryId.equalsIgnoreCase("queryId")) {
      String updatedQueryId = queryId
          .substring(queryId.lastIndexOf(
              CarbonCommonConstants.UNDERSCORE));
      String serializedloadModel = CarbonProperties.getInstance().getProperty(updatedQueryId);
      if (null != serializedloadModel) {
        LOG.info("Setting Load model to Configuration and query id is: " + updatedQueryId);
        configuration.set(CarbonTableOutputFormat.LOAD_MODEL, serializedloadModel);
      }
    }
  }
}
