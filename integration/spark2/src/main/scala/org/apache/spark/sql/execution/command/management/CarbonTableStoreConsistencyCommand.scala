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

package org.apache.spark.sql.execution.command.management

import scala.collection.mutable

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath

case class CarbonTableStoreConsistencyCommand
(databaseNameOp: Option[String]) extends DataCommand {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("TableName", StringType, nullable = false)(),
      AttributeReference("MissingSegmentDirs", StringType, nullable = false)(),
      AttributeReference("isSegmentFileExists", StringType, nullable = false)(),
      AttributeReference("isDataAndIndexFileExists", StringType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    var isSegmentFileExists = false
    var isDataAndIndexFileExists = false
    val segmentDirsNotPresent = mutable.ListBuffer[String]()
    val outPutRows = mutable.ListBuffer[Row]()
    var carbonTable: CarbonTable = null
    val tables = sparkSession.catalog
      .listTables(databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)).collect()
    tables.foreach { table =>
      val isTableExists = CarbonEnv.getInstance(sparkSession)
        .carbonMetaStore
        .tableExists(table.name, Some(table.database))(sparkSession)
      if (isTableExists) {
        carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, table.name)(sparkSession)
        val loadMetadataDetails = SegmentStatusManager.readTableStatusFile(CarbonTablePath
          .getTableStatusFilePath(
          carbonTable.getTablePath))
        if (loadMetadataDetails.nonEmpty) {
          loadMetadataDetails.foreach { loadMetadataDetail =>
            val segmentPath = CarbonTablePath.getSegmentPath(carbonTable.getTablePath,
              loadMetadataDetail.getLoadName)

            val segmentDir = FileFactory.getCarbonFile(segmentPath)
            if (!segmentDir.isFileExist) {
              segmentDirsNotPresent += segmentDir.getName
            } else {
              if (segmentDir.getSize >= 0) {
                isDataAndIndexFileExists = true;
              }
            }
            val segmentFilePath = FileFactory.getCarbonFile(CarbonTablePath.getSegmentFilePath(
              carbonTable.getTablePath,
              loadMetadataDetail.getSegmentFile))
            if (segmentFilePath.isFileExist) {
              isSegmentFileExists = true
            }
            if (segmentDirsNotPresent.nonEmpty || !isSegmentFileExists ||
                !isDataAndIndexFileExists) {
              outPutRows += Row(
                table.name,
                segmentDirsNotPresent.mkString(","),
                isSegmentFileExists.toString,
                isDataAndIndexFileExists.toString
              )
            }
            segmentDirsNotPresent.clear()
            isSegmentFileExists = false
            isDataAndIndexFileExists = false
          }
        }
        outPutRows
      } else {
        Seq.empty
      }
    }
    outPutRows
  }

  override protected def opName: String = "TABLE STORE CONSISTENCY"
}
