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

import java.util.concurrent.ExecutorService;

import com.facebook.presto.hive.ForHiveClient;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HiveMetadataFactory;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.LocationService;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.PartitionUpdate;
import com.facebook.presto.hive.TableParameterCodec;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.statistics.MetastoreHiveStatisticsProvider;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.joda.time.DateTimeZone;

public class CarbonMetadataFactory extends HiveMetadataFactory {

  private static final Logger log = Logger.get(HiveMetadataFactory.class);
  private final boolean allowCorruptWritesForTesting;
  private final boolean skipDeletionForAlter;
  private final boolean skipTargetCleanupOnRollback;
  private final boolean writesToNonManagedTablesEnabled;
  private final boolean createsOfNonManagedTablesEnabled;
  private final long perTransactionCacheMaximumSize;
  private final int maxPartitions;
  private final ExtendedHiveMetastore metastore;
  private final HdfsEnvironment hdfsEnvironment;
  private final HivePartitionManager partitionManager;
  private final DateTimeZone timeZone;
  private final int maxConcurrentFileRenames;
  private final TypeManager typeManager;
  private final LocationService locationService;
  private final TableParameterCodec tableParameterCodec;
  private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
  private final ExecutorService executorService;
  private final BoundedExecutor renameExecution;
  private final TypeTranslator typeTranslator;
  private final String prestoVersion;

  @Inject
  public CarbonMetadataFactory(HiveClientConfig hiveClientConfig, ExtendedHiveMetastore metastore,
      HdfsEnvironment hdfsEnvironment, HivePartitionManager partitionManager,
      @ForHiveClient ExecutorService executorService, TypeManager typeManager, LocationService locationService,
      TableParameterCodec tableParameterCodec, JsonCodec<PartitionUpdate> partitionUpdateCodec,
      TypeTranslator typeTranslator, NodeVersion nodeVersion) {
    this(metastore, hdfsEnvironment, partitionManager, hiveClientConfig.getDateTimeZone(),
        hiveClientConfig.getMaxConcurrentFileRenames(),
        hiveClientConfig.getAllowCorruptWritesForTesting(),
        hiveClientConfig.isSkipDeletionForAlter(), hiveClientConfig.isSkipTargetCleanupOnRollback(),
        hiveClientConfig.getWritesToNonManagedTablesEnabled(),
        hiveClientConfig.getCreatesOfNonManagedTablesEnabled(),
        hiveClientConfig.getPerTransactionMetastoreCacheMaximumSize(),
        hiveClientConfig.getMaxPartitionsPerScan(), typeManager, locationService,
        tableParameterCodec, partitionUpdateCodec, executorService, typeTranslator,
        nodeVersion.toString());
  }

  public CarbonMetadataFactory(ExtendedHiveMetastore metastore, HdfsEnvironment hdfsEnvironment,
      HivePartitionManager partitionManager, DateTimeZone timeZone, int maxConcurrentFileRenames,
      boolean allowCorruptWritesForTesting, boolean skipDeletionForAlter,
      boolean skipTargetCleanupOnRollback, boolean writesToNonManagedTablesEnabled,
      boolean createsOfNonManagedTablesEnabled, long perTransactionCacheMaximumSize,
      int maxPartitions, TypeManager typeManager, LocationService locationService,
      TableParameterCodec tableParameterCodec, JsonCodec<PartitionUpdate> partitionUpdateCodec,
      ExecutorService executorService, TypeTranslator typeTranslator, String prestoVersion) {

    super(metastore, hdfsEnvironment, partitionManager, timeZone, maxConcurrentFileRenames,
        allowCorruptWritesForTesting, skipDeletionForAlter, skipTargetCleanupOnRollback,
        writesToNonManagedTablesEnabled, createsOfNonManagedTablesEnabled,
        perTransactionCacheMaximumSize, maxPartitions, typeManager, locationService,
        tableParameterCodec, partitionUpdateCodec, executorService, typeTranslator, prestoVersion);
    this.metastore = metastore;
    this.hdfsEnvironment = hdfsEnvironment;
    this.partitionManager = partitionManager;
    this.timeZone = timeZone;
    this.maxConcurrentFileRenames = maxConcurrentFileRenames;
    this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;
    this.skipDeletionForAlter = skipDeletionForAlter;
    this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
    this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
    this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
    this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;
    this.maxPartitions = maxPartitions;
    this.typeManager = typeManager;
    this.locationService = locationService;
    this.tableParameterCodec = tableParameterCodec;
    this.partitionUpdateCodec = partitionUpdateCodec;
    this.executorService = executorService;
    this.typeTranslator = typeTranslator;
    this.prestoVersion = prestoVersion;
    this.renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
  }

  @Override public HiveMetadata get() {
    SemiTransactionalHiveMetastore metastore =
        new SemiTransactionalHiveMetastore(this.hdfsEnvironment, CachingHiveMetastore
            .memoizeMetastore(this.metastore, this.perTransactionCacheMaximumSize),
            this.renameExecution, this.skipDeletionForAlter, this.skipTargetCleanupOnRollback);
    return new CarbondataMetadata(metastore, hdfsEnvironment, partitionManager, timeZone,
        allowCorruptWritesForTesting, writesToNonManagedTablesEnabled,
        createsOfNonManagedTablesEnabled, typeManager, locationService, tableParameterCodec,
        partitionUpdateCodec, typeTranslator, prestoVersion,
        new MetastoreHiveStatisticsProvider(metastore), maxPartitions);
  }
}
