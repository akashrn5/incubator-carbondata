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
package org.apache.carbondata.mv.datamap

import java.io.IOException
import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateUtil
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesUtil
import org.apache.spark.sql.execution.datasources.FindDataSourceTable
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException,
  MalformedDataMapCommandException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.MV_TIMESERIES
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

class MVTimeSeriesDataMapProvider(mainTable: CarbonTable,
    sparkSession: SparkSession,
    dataMapSchema: DataMapSchema)
  extends MVDataMapProvider(mainTable, sparkSession, dataMapSchema) {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  @throws[MalformedDataMapCommandException]
  @throws[IOException]
  override def initMeta(ctasSqlStatement: String): Unit = {
    if (dataMapSchema.isLazy) {
      throw new MalformedCarbonCommandException(
        "MV TimeSeries datamap does not support Lazy Rebuild")
    }
    val dmProperties: util.Map[String, String] = dataMapSchema.getProperties
    TimeSeriesUtil.validateDMPropertiesForTimeSeries(dmProperties.asScala.toMap)
    super.initMeta(ctasSqlStatement)
  }

  @throws[IOException]
  override def rebuildInternal(newLoadName: String,
      segmentMap: java.util.Map[String, java.util.List[String]],
      dataMapTable: CarbonTable): Boolean = {
    val ctasQuery = dataMapSchema.getCtasQuery
    if (ctasQuery != null) {
      val identifier = dataMapSchema.getRelationIdentifier
      val updatedQuery = new CarbonSpark2SqlParser().addPreAggFunction(ctasQuery)
      val queryPlan = SparkSQLUtil.execute(
        sparkSession.sql(updatedQuery).queryExecution.analyzed,
        sparkSession).drop("preAgg")
      val header = dataMapTable.getTableInfo.getFactTable.getListOfColumns.asScala
        .filter { column =>
          !column.getColumnName
            .equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE)
        }.sortBy(_.getSchemaOrdinal).map(_.getColumnName).mkString(",")
      val loadCommand = CarbonLoadDataCommand(
        databaseNameOp = Some(identifier.getDatabaseName),
        tableName = identifier.getTableName,
        factPathFromUser = null,
        dimFilesPath = Seq(),
        options = scala.collection.immutable.Map("fileheader" -> header),
        false,
        inputSqlString = null,
        dataFrame = Some(queryPlan),
        updateModel = None,
        tableInfoOp = None,
        internalOptions = Map("mergedSegmentName" -> newLoadName,
          CarbonCommonConstants.IS_INTERNAL_LOAD_CALL -> "true"),
        partition = Map.empty)

      try {
        SparkSQLUtil.execute(loadCommand, sparkSession)
      } catch {
        case ex: Exception =>
          // If load to dataMap table fails, disable the dataMap and if newLoad is still
          // in INSERT_IN_PROGRESS state, mark for delete the newLoad and update table status file
          DataMapStatusManager.disableDataMap(dataMapSchema.getDataMapName)
          LOGGER.error("Data Load failed for DataMap: ", ex)
          CarbonLoaderUtil.updateTableStatusInCaseOfFailure(
            newLoadName,
            dataMapTable.getAbsoluteTableIdentifier,
            dataMapTable.getTableName,
            dataMapTable.getDatabaseName,
            dataMapTable.getTablePath,
            dataMapTable.getMetadataPath)
          throw ex
      }
    }
    true
  }

}
