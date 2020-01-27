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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;

import org.apache.carbondata.hadoop.api.CarbonOutputCommitter;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveInsertTableHandle;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HiveUtil;
import com.facebook.presto.hive.LocationService;
import com.facebook.presto.hive.PartitionUpdate;
import com.facebook.presto.hive.TableParameterCodec;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.statistics.HiveStatisticsProvider;
import com.facebook.presto.hive.util.ConfigurationUtils;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.joda.time.DateTimeZone;

public class CarbondataMetadata extends HiveMetadata {

  private HdfsEnvironment hdfsEnvironment;
  private SemiTransactionalHiveMetastore metastore;

  public CarbondataMetadata(SemiTransactionalHiveMetastore metastore,
      HdfsEnvironment hdfsEnvironment, HivePartitionManager partitionManager, DateTimeZone timeZone,
      boolean allowCorruptWritesForTesting, boolean writesToNonManagedTablesEnabled,
      boolean createsOfNonManagedTablesEnabled, TypeManager typeManager,
      LocationService locationService, TableParameterCodec tableParameterCodec,
      JsonCodec<PartitionUpdate> partitionUpdateCodec, TypeTranslator typeTranslator,
      String prestoVersion, HiveStatisticsProvider hiveStatisticsProvider, int maxPartitions) {
    super(metastore, hdfsEnvironment, partitionManager, timeZone, allowCorruptWritesForTesting,
        writesToNonManagedTablesEnabled, createsOfNonManagedTablesEnabled, typeManager,
        locationService, tableParameterCodec, partitionUpdateCodec, typeTranslator, prestoVersion,
        hiveStatisticsProvider, maxPartitions);
    this.hdfsEnvironment  = hdfsEnvironment;
    this.metastore = metastore;
  }

  @Override public HiveInsertTableHandle beginInsert(ConnectorSession session,
      ConnectorTableHandle tableHandle) {
    HiveInsertTableHandle hiveInsertTableHandle = super.beginInsert(session, tableHandle);
//    SchemaTableName tableName = HiveUtil.schemaTableName(tableHandle);
//    Optional<Table> table =
//        this.metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
//    Path outputPath = new Path(hiveInsertTableHandle.getLocationHandle().getJsonSerializableTargetPath());
//    JobConf conf = ConfigurationUtils.toJobConf(this.hdfsEnvironment.getConfiguration(
//        new HdfsEnvironment.HdfsContext(session, table.get().getDatabaseName(),
//            table.get().getTableName()),
//        outputPath));
//    JobContextImpl jobContext = new JobContextImpl(conf, new JobID());
//    TaskAttemptID taskAttemptID = TaskAttemptID.forName(conf.get("mapred.task.id"));
////    CarbonTableOutputFormat.setLoadModel(conf, carbonLoadModel);
//    if (taskAttemptID == null) {
//      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
//      String jobTrackerId = formatter.format(new Date());
//      taskAttemptID = new TaskAttemptID(jobTrackerId, 0, TaskType.MAP, 0, 0);
//    }
//    TaskAttemptContextImpl context = new TaskAttemptContextImpl(conf, taskAttemptID);
//    OutputCommitter carbonOutputCommitter = null;
//    try {
//      carbonOutputCommitter = new CarbonOutputCommitter(outputPath, context);
//      carbonOutputCommitter.setupJob(jobContext);
//    } catch (IOException e) {
//      throw new RuntimeException("error setting the output committer");
//    }
    return hiveInsertTableHandle;
  }

  @Override public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
      ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments,
      Collection<ComputedStatistics> computedStatistics) {
    Optional<ConnectorOutputMetadata> connectorOutputMetadata =
        super.finishInsert(session, insertHandle, fragments, computedStatistics);
    return connectorOutputMetadata;
  }
}
