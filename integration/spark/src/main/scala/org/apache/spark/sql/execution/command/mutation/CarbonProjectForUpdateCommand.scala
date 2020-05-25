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

package org.apache.spark.sql.execution.command.mutation

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{ProjectExec, RowDataSourceScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.{CarbonInsertIntoWithDf, CommonLoadUtils}
import org.apache.spark.sql.execution.command.mutation.DeleteExecution.checkAndUpdateStatusFiles
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.strategy.{CarbonDataSourceScan, MixedFormatHandler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types.{ArrayType, LongType}
import org.apache.spark.storage.StorageLevel

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.data.RowCountDetailsVO
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, DeleteDeltaBlockDetails, SegmentUpdateDetails, TupleIdEnum}
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.writer.CarbonDeleteDeltaWriterImpl
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, UpdateTablePostEvent, UpdateTablePreEvent}
import org.apache.carbondata.indexserver.DistributedRDDUtils
import org.apache.carbondata.processing.exception.MultipleMatchingException
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.sdk.file.{CarbonReader, CarbonWriterBuilder}
import org.apache.carbondata.spark.DeleteDelataResultImpl
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory.{LOGGER, updateSegmentFiles}
import org.apache.carbondata.spark.rdd.CarbonScanRDD
import org.apache.carbondata.view.MVManagerInSpark

private[sql] case class CarbonProjectForUpdateCommand(
    plan: LogicalPlan,
    databaseNameOp: Option[String],
    tableName: String,
    columns: List[String])
  extends DataCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("Updated Row Count", LongType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    var updatedRowCount = 0L
    IUDCommonUtil.checkIfSegmentListIsSet(sparkSession, plan)
    val res = plan find {
      case relation: LogicalRelation if relation.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        true
      case _ => false
    }

    if (res.isEmpty) {
      return Array(Row(updatedRowCount)).toSeq
    }
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    setAuditInfo(Map("plan" -> plan.simpleString))
    columns.foreach { col =>
      val dataType = carbonTable.getColumnByName(col).getColumnSchema.getDataType
      if (dataType.isComplexType) {
        throw new UnsupportedOperationException("Unsupported operation on Complex data type")
      }

    }
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "loading", "data update")
    }

    if (!carbonTable.canAllow(carbonTable, TableOperation.UPDATE)) {
      throw new MalformedCarbonCommandException(
        "update operation is not supported for index")
    }

    // Block the update operation for non carbon formats
    if (MixedFormatHandler.otherFormatSegmentsExist(carbonTable.getMetadataPath)) {
      throw new MalformedCarbonCommandException(
        s"Unsupported update operation on table containing mixed format segments")
    }

    // trigger event for Update table
    val operationContext = new OperationContext
    val updateTablePreEvent: UpdateTablePreEvent =
      UpdateTablePreEvent(sparkSession, carbonTable)
    operationContext.setProperty("isLoadOrCompaction", false)
    OperationListenerBus.getInstance.fireEvent(updateTablePreEvent, operationContext)
    val metadataLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.METADATA_LOCK)
    val compactionLock = CarbonLockFactory.getCarbonLockObj(carbonTable
      .getAbsoluteTableIdentifier, LockUsage.COMPACTION_LOCK)
    val updateLock = CarbonLockFactory.getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
      LockUsage.UPDATE_LOCK)
    var lockStatus = false
    // get the current time stamp which should be same for delete and update.
    val currentTime = CarbonUpdateUtil.readCurrentTime
    //    var dataFrame: DataFrame = null
    var dataSet: DataFrame = null
    val isPersistEnabled = CarbonProperties.getInstance.isPersistUpdateDataset
    try {
      lockStatus = metadataLock.lockWithRetries()
      if (lockStatus) {
        logInfo("Successfully able to get the table metadata file lock")
      }
      else {
        throw new Exception("Table is locked for updation. Please try after some time")
      }

      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      if (updateLock.lockWithRetries(3, 3)) {
        if (compactionLock.lockWithRetries(3, 3)) {
          // Get RDD.
          dataSet = if (isPersistEnabled) {
            Dataset.ofRows(sparkSession, plan).persist(StorageLevel.fromString(
              CarbonProperties.getInstance.getUpdateDatasetStorageLevel()))
          }
          else {
            Dataset.ofRows(sparkSession, plan)
          }
          val newFlow = true
          if (newFlow) {
            val sparkPlan = dataSet.queryExecution.sparkPlan
            //            val mainTableRDD: Seq[RDD[InternalRow]] = sparkPlan.collect {
            //              case batchData: CarbonDataSourceScan =>
            //                batchData.rdd
            //              case rowData: RowDataSourceScanExec =>
            //                rowData.rdd
            //            }
            val projections = plan.output.map(_.name)
            val updatedColumn = projections
              .find { s => s.endsWith(CarbonCommonConstants.UPDATED_COL_EXTENSION) }
              .get
            var valueToBeUpdated: String = null
            plan match {
              case p: Project =>
                p.projectList.foreach {
                  case a@Alias(child, _) =>
                    child match {
                      case literal: Literal =>
                        valueToBeUpdated = literal.toString()
                      case _ =>
                    }
                  case _ =>
                }
            }
            val indexToUpdated = projections.indexOf(updatedColumn)
            val updatedProjections = projections.updated(indexToUpdated,
              updatedColumn.substring(0,
                updatedColumn.indexOf(CarbonCommonConstants.UPDATED_COL_EXTENSION)))
            val mainTableRDD = sparkPlan
              .asInstanceOf[InMemoryTableScanExec]
              .relation
              .child
              .asInstanceOf[WholeStageCodegenExec]
              .child
              .asInstanceOf[ProjectExec]
              .child
              .asInstanceOf[CarbonDataSourceScan]
              .rdd
            val csrdd = mainTableRDD.asInstanceOf[CarbonScanRDD[InternalRow]]
            val filterExpression = csrdd.indexFilter.getExpression

            // ********** create reader and read the rows for deletion*******************
            val reader = CarbonReader.builder(carbonTable.getTablePath, carbonTable.getTableName)
              .projection(updatedProjections.toArray)
              .filter(filterExpression)
              .build()
            var count = 0
            var row: Array[AnyRef] = null
            var rowsToWrite: List[Array[AnyRef]] = List()
            var tupleIDs: ArrayBuffer[String] = new ArrayBuffer[String]()
            while ( { reader.hasNext }) {
              row = reader.readNextRow.asInstanceOf[Array[AnyRef]]
              tupleIDs += row(row.length - 1).asInstanceOf[String]
              row(indexToUpdated) = valueToBeUpdated
              rowsToWrite = rowsToWrite :+ row
              count += 1
            }
            reader.close()


            // *********************** perform delete operation and write the delete delta file and update table status file********************
            //get blockRowCount
            val (carbonInputFormat, job) = DeleteExecution.createCarbonInputFormat(carbonTable
              .getAbsoluteTableIdentifier)
            val blockMappingVO =
              carbonInputFormat.getBlockRowCount(
                job,
                carbonTable,
                CarbonFilters.getPartitions(
                  Seq.empty,
                  sparkSession,
                  TableIdentifier(tableName, databaseNameOp)).map(_.asJava).orNull, true)

            val segmentUpdateStatusMngr = new SegmentUpdateStatusManager(carbonTable)
            CarbonUpdateUtil
              .createBlockDetailsMap(blockMappingVO, segmentUpdateStatusMngr)
            val metadataDetails = SegmentStatusManager.readTableStatusFile(
              CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath))
            val isStandardTable = CarbonUtil.isStandardCarbonTable(carbonTable)
            val blockDetails = blockMappingVO.getBlockToSegmentMapping


            // val a = create a tuple/grouped rows of key/value (seg/blockId -> tupleIDs )
            var keyToTupleIds: scala.collection.mutable.Map[String, List[String]] = scala
              .collection
              .mutable
              .Map
              .empty[String, List[String]]
            tupleIDs.foreach { tupleID =>
              val key = CarbonUpdateUtil.getSegmentWithBlockFromTID(tupleID,
                carbonTable.isHivePartitionTable)
              if (keyToTupleIds.get(key).isEmpty) {
                var rowIDs: List[String] = List()
                rowIDs = rowIDs :+ tupleID
                keyToTupleIds += (key -> rowIDs)
              } else {
                var rowIDs = keyToTupleIds(key)
                rowIDs = rowIDs :+ tupleID
                keyToTupleIds += (key -> rowIDs)
              }
            }

            // val b = create a tuple of same key/value (seg/blockId -> RowCountDetailsVO), can get the key from a
            var keyToRowDetailsVO: List[(String, RowCountDetailsVO)] = List()
            keyToTupleIds.foreach { key =>
              keyToRowDetailsVO = keyToRowDetailsVO :+
                                  (key._1, blockMappingVO.getCompleteBlockRowDetailVO.get(key._1))
            }

            // join a and b
            var joinedKeyToTupleIdsAndRwoDetails: scala.collection.mutable.Map[String, (
              RowCountDetailsVO, List[String])] = scala
              .collection
              .mutable
              .Map[String, (RowCountDetailsVO, List[String])]()

            keyToRowDetailsVO.foreach { value =>
              joinedKeyToTupleIdsAndRwoDetails += (value._1 -> (value._2, keyToTupleIds(value._1)))
            }

            var result: List[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]] = List()
            joinedKeyToTupleIdsAndRwoDetails.foreach { value =>
              result = result :+
                       deleteDeltaFunc(value._1,
                         value._2._2,
                         currentTime.toString,
                         value._2._1,
                         isStandardTable,
                         metadataDetails
                           .find(_.getLoadName.equalsIgnoreCase(blockDetails.get(value._1)))
                           .get, carbonTable.isHivePartitionTable, carbonTable)
            }

            // write update status files
            //            if (result.flatten.isEmpty) {
            //              return (Seq.empty[Segment], operatedRowCount)
            //            }
            // update new status file
            checkAndUpdateStatusFiles(ExecutionErrors(FailureCauses.NONE, ""),
              result.toArray, carbonTable, currentTime.toString,
              blockMappingVO, true)


            // **************** perform update operation, write new files and update the metadata.

            // prepare load model
            val carbonRelation: CarbonDatasourceHadoopRelation = res match {
              case Some(relation: LogicalRelation) =>
                relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
              case _ => sys.error("")
            }

            val header = getHeader(carbonRelation, plan)
            val options = Map(("fileheader" -> header))
            val configuration = sparkSession.sessionState.newHadoopConf()
            val carbonLoadModel: CarbonLoadModel = CommonLoadUtils.prepareLoadModel(
              configuration,
              "",
              CommonLoadUtils.getFinalLoadOptions(carbonTable, options),
              parentTablePath = null,
              table = carbonTable,
              isDataFrame = true,
              internalOptions = Map.empty,
              partition = Map.empty,
              options = options)

            // TODO: here get the segments to be updated, for each segment get the Segment Object.
            val ssm = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)
            val seg = ssm.getValidAndInvalidSegments().getValidSegments.get(0)

            val updateTableModel = UpdateTableModel(true, currentTime, executionErrors, Seq())

            val taskNo =
              CarbonUpdateUtil.getLatestTaskIdForSegment(seg, carbonLoadModel.getTablePath) + 1
            val writer = new CarbonWriterBuilder().withCsvInput()
              .withLoadOptions(options.asJava)
              .withHadoopConf(configuration)
              .uniqueIdentifier(currentTime)
              .taskNo(taskNo)
              .outputPath(carbonTable.getSegmentPath("0"))
              .segmentNo("0")
              .withSchemaFile(CarbonTablePath.getSchemaFilePath(carbonTable.getTablePath))
              .writtenBy("update")
              .build()

            // TODO: skip writing the tupleIDs in row
            rowsToWrite.foreach { row =>
              writer.write(row)
            }
            writer.close()

            // after writing success, update the table status file about the update information.
            // if no updated records, or select rows are zero, then no need to update the status
            // file
            // **********************update table status and segment file **********************************
            // refer integration/spark/src/main/scala/org/apache/carbondata/spark/rdd/CarbonDataRDDFactory.scala:487
            // updated timestamp is the current time in update load model
            // update the segment file and merge to single segment file
            // update the  table status file using org.apache.carbondata.core.mutate.CarbonUpdateUtil.updateTableMetadataStatus
            val segmentDetails = new util.HashSet[Segment]()
            var resultSize = 0
            segmentDetails.add(new Segment("0"))
            var segmentMetaDataInfoMap = scala
              .collection
              .mutable
              .Map
              .empty[String, SegmentMetaDataInfo]

            val segmentFiles = updateSegmentFiles(carbonTable,
              segmentDetails,
              updateTableModel,
              segmentMetaDataInfoMap.asJava)

            // this means that the update doesnt have any records to update so no need to do table
            // status file updation.
            //            if (resultSize == 0) {
            //              return null
            //            }
            if (!CarbonUpdateUtil.updateTableMetadataStatus(
              segmentDetails,
              carbonTable,
              updateTableModel.updatedTimeStamp + "",
              true,
              new util.ArrayList[Segment](0),
              new util.ArrayList[Segment](segmentFiles), "")) {
              LOGGER.error("Data update failed due to failure in table status updation.")
              updateTableModel.executorErrors.errorMsg = "errorMessage"
              updateTableModel.executorErrors.failureCauses = FailureCauses
                .STATUS_FILE_UPDATION_FAILURE
              return null
            }
            // code to handle Pre-Priming cache for update command
            if (!segmentFiles.isEmpty) {
              val segmentsToPrePrime = segmentFiles
                .asScala
                .map(iterator => iterator.getSegmentNo)
                .toSeq
              DistributedRDDUtils
                .triggerPrepriming(sparkSession, carbonTable, segmentsToPrePrime,
                  operationContext, configuration, segmentsToPrePrime.toList)
            }

          } else {
          if (CarbonProperties.isUniqueValueCheckEnabled) {
            // If more than one value present for the update key, should fail the update
            val ds = dataSet.select(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)
              .groupBy(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)
              .count()
              .select("count")
              .filter(col("count") > lit(1))
              .limit(1)
              .collect()
            // tupleId represents the source rows that are going to get replaced.
            // If same tupleId appeared more than once means key has more than one value to replace.
            // which is undefined behavior.
            if (ds.length > 0 && ds(0).getLong(0) > 1) {
              throw new UnsupportedOperationException(
                " update cannot be supported for 1 to N mapping, as more than one value present " +
                "for the update key")
            }
          }
          // handle the clean up of IUD.
          CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false)

          // do delete operation.
          val (segmentsToBeDeleted, updatedRowCountTemp) = DeleteExecution.deleteDeltaExecution(
            databaseNameOp,
            tableName,
            sparkSession,
            dataSet.rdd,
            currentTime + "",
            isUpdateOperation = true,
            executionErrors)

          if (executionErrors.failureCauses != FailureCauses.NONE) {
            throw new Exception(executionErrors.errorMsg)
          }

          updatedRowCount = updatedRowCountTemp
          // do update operation.
          performUpdate(dataSet,
            databaseNameOp,
            tableName,
            plan,
            sparkSession,
            currentTime,
            executionErrors,
            segmentsToBeDeleted)

          // prepriming for update command
          DeleteExecution.reloadDistributedSegmentCache(carbonTable,
            segmentsToBeDeleted, operationContext)(sparkSession)

        }
        } else {
          throw new ConcurrentOperationException(carbonTable, "compaction", "update")
        }
      } else {
        throw new ConcurrentOperationException(carbonTable, "update/delete", "update")
      }
      if (executionErrors.failureCauses != FailureCauses.NONE) {
        throw new Exception(executionErrors.errorMsg)
      }

      // Do IUD Compaction.
      HorizontalCompaction.tryHorizontalCompaction(
        sparkSession, carbonTable, isUpdateOperation = true)

      // Truncate materialized views on the current table.
      val viewManager = MVManagerInSpark.get(sparkSession)
      val viewSchemas = viewManager.getSchemasOnTable(carbonTable)
      if (!viewSchemas.isEmpty) {
        viewManager.onTruncate(viewSchemas)
      }

      // trigger event for Update table
      val updateTablePostEvent: UpdateTablePostEvent =
        UpdateTablePostEvent(sparkSession, carbonTable)
      OperationListenerBus.getInstance.fireEvent(updateTablePostEvent, operationContext)
    } catch {
      case e: HorizontalCompactionException =>
        LOGGER.error(
          "Update operation passed. Exception in Horizontal Compaction. Please check logs." + e)
        // In case of failure , clean all related delta files
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, e.compactionTimeStamp.toString)

      case e: Exception =>
        LOGGER.error("Exception in update operation", e)
        // ****** start clean up.
        // In case of failure , clean all related delete delta files
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, currentTime + "")

        // *****end clean up.
        if (null != e.getMessage) {
          sys.error("Update operation failed. " + e.getMessage)
        }
        if (null != e.getCause && null != e.getCause.getMessage) {
          sys.error("Update operation failed. " + e.getCause.getMessage)
        }
        sys.error("Update operation failed. please check logs.")
    } finally {
      if (null != dataSet && isPersistEnabled) {
        dataSet.unpersist()
      }
      updateLock.unlock()
      compactionLock.unlock()
      if (lockStatus) {
        CarbonLockUtil.fileUnlock(metadataLock, LockUsage.METADATA_LOCK)
      }
    }
    Seq(Row(updatedRowCount))
  }

  private def performUpdate(
      dataFrame: Dataset[Row],
      databaseNameOp: Option[String],
      tableName: String,
      plan: LogicalPlan,
      sparkSession: SparkSession,
      currentTime: Long,
      executorErrors: ExecutionErrors,
      deletedSegments: Seq[Segment]): Unit = {

    def isDestinationRelation(relation: CarbonDatasourceHadoopRelation): Boolean = {
      val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
      (databaseNameOp.isDefined &&
       databaseNameOp.get == dbName &&
       tableName == relation.identifier.getCarbonTableIdentifier.getTableName) ||
      (tableName == relation.identifier.getCarbonTableIdentifier.getTableName)
    }

    // from the dataFrame schema iterate through all the column to be updated and
    // check for the data type, if the data type is complex then throw exception
    def checkForUnsupportedDataType(dataFrame: DataFrame): Unit = {
      dataFrame.schema.foreach(col => {
        // the new column to be updated will be appended with "-updatedColumn" suffix
        if (col.name.endsWith(CarbonCommonConstants.UPDATED_COL_EXTENSION) &&
            col.dataType.isInstanceOf[ArrayType]) {
          throw new UnsupportedOperationException("Unsupported data type: Array")
        }
      })
    }


    // check for the data type of the new value to be updated
    checkForUnsupportedDataType(dataFrame)
    val ex = dataFrame.queryExecution.analyzed
    val res = ex find {
      case relation: LogicalRelation
        if relation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           isDestinationRelation(relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]) =>
        true
      case _ => false
    }
    val carbonRelation: CarbonDatasourceHadoopRelation = res match {
      case Some(relation: LogicalRelation) =>
        relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
      case _ => sys.error("")
    }

    val updateTableModel = UpdateTableModel(true, currentTime, executorErrors, deletedSegments)

    val header = getHeader(carbonRelation, plan)

    CarbonInsertIntoWithDf(
      databaseNameOp = Some(carbonRelation.identifier.getCarbonTableIdentifier.getDatabaseName),
      tableName = carbonRelation.identifier.getCarbonTableIdentifier.getTableName,
      options = Map(("fileheader" -> header)),
      isOverwriteTable = false,
      dataFrame = dataFrame,
      updateModel = Some(updateTableModel)).process(sparkSession)

    executorErrors.errorMsg = updateTableModel.executorErrors.errorMsg
    executorErrors.failureCauses = updateTableModel.executorErrors.failureCauses
  }

  def getHeader(relation: CarbonDatasourceHadoopRelation, plan: LogicalPlan): String = {
    var header = ""
    var found = false

    plan match {
      case Project(pList, _) if (!found) =>
        found = true
        header = pList
          .filter(field => !field.name
            .equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
          .map(col => if (col.name.endsWith(CarbonCommonConstants.UPDATED_COL_EXTENSION)) {
            col.name
              .substring(0, col.name.lastIndexOf(CarbonCommonConstants.UPDATED_COL_EXTENSION))
          }
          else {
            col.name
          }).mkString(",")
    }
    header
  }

  def deleteDeltaFunc(key: String,
      tupleIds: List[String],
      timestamp: String,
      rowCountDetailsVO: RowCountDetailsVO,
      isStandardTable: Boolean,
      load: LoadMetadataDetails, isPartitionTable: Boolean, carbontable: CarbonTable
  ): List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))] = {
    var output: List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))] = List()
    val result = new DeleteDelataResultImpl()
    var deleteStatus = SegmentStatus.LOAD_FAILURE
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    // here key = segment/blockName
    val blockName = if (isPartitionTable) {
      CarbonUpdateUtil.getBlockName(CarbonTablePath.addDataPartPrefix(key))
    } else {
      CarbonUpdateUtil
        .getBlockName(
          CarbonTablePath.addDataPartPrefix(key.split(CarbonCommonConstants.FILE_SEPARATOR)(1)))
    }
    val deleteDeltaBlockDetails: DeleteDeltaBlockDetails = new DeleteDeltaBlockDetails(blockName)
    val segmentUpdateDetails = new SegmentUpdateDetails()
    var TID = ""
    var countOfRows = 0

    tupleIds.foreach { tupleId =>
      TID = tupleId
      val (offset, blockletId, pageId) = if (isPartitionTable) {
        (CarbonUpdateUtil.getRequiredFieldFromTID(TID,
          TupleIdEnum.OFFSET.getTupleIdIndex - 1),
          CarbonUpdateUtil.getRequiredFieldFromTID(TID,
            TupleIdEnum.BLOCKLET_ID.getTupleIdIndex - 1),
          Integer.parseInt(CarbonUpdateUtil.getRequiredFieldFromTID(TID,
            TupleIdEnum.PAGE_ID.getTupleIdIndex - 1)))
      } else {
        (CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.OFFSET),
          CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.BLOCKLET_ID),
          Integer.parseInt(CarbonUpdateUtil.getRequiredFieldFromTID(TID,
            TupleIdEnum.PAGE_ID)))
      }
      val IsValidOffset = deleteDeltaBlockDetails.addBlocklet(blockletId, offset, pageId)
      // stop delete operation
      if (!IsValidOffset) {
        //        executorErrors.failureCauses = FailureCauses.MULTIPLE_INPUT_ROWS_MATCHING
        //        executorErrors.errorMsg = "Multiple input rows matched for same row."
        throw new MultipleMatchingException("Multiple input rows matched for same row.")
      }
      countOfRows = countOfRows + 1
    }
//    }
    try {


      val blockPath =
        if (StringUtils.isNotEmpty(load.getPath)) {
          load.getPath
        } else {
          CarbonUpdateUtil.getTableBlockPath(TID, carbontable.getTablePath, isStandardTable)
        }
      val completeBlockName = if (isPartitionTable) {
        CarbonTablePath
          .addDataPartPrefix(
            CarbonUpdateUtil.getRequiredFieldFromTID(TID,
              TupleIdEnum.BLOCK_ID.getTupleIdIndex - 1) +
            CarbonCommonConstants.FACT_FILE_EXT)
      } else {
        CarbonTablePath
          .addDataPartPrefix(
            CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.BLOCK_ID) +
            CarbonCommonConstants.FACT_FILE_EXT)
      }
      val deleteDeletaPath = CarbonUpdateUtil
        .getDeleteDeltaFilePath(blockPath, blockName, timestamp)
      val carbonDeleteWriter = new CarbonDeleteDeltaWriterImpl(deleteDeletaPath)


      segmentUpdateDetails.setBlockName(blockName)
      segmentUpdateDetails.setActualBlockName(completeBlockName)
      segmentUpdateDetails.setSegmentName(load.getLoadName)
      segmentUpdateDetails.setDeleteDeltaEndTimestamp(timestamp)
      segmentUpdateDetails.setDeleteDeltaStartTimestamp(timestamp)

      val alreadyDeletedRows: Long = rowCountDetailsVO.getDeletedRowsInBlock
      val totalDeletedRows: Long = alreadyDeletedRows + countOfRows
      segmentUpdateDetails.setDeletedRowsInBlock(totalDeletedRows.toString)
      if (totalDeletedRows == rowCountDetailsVO.getTotalNumberOfRows) {
        segmentUpdateDetails.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE)
      }
      else {
        // write the delta file
        carbonDeleteWriter.write(deleteDeltaBlockDetails)
      }

      deleteStatus = SegmentStatus.SUCCESS
      output = output :+ (deleteStatus, (segmentUpdateDetails, ExecutionErrors(FailureCauses.NONE, ""), countOfRows.toLong))
      output
    } catch {
      case e: MultipleMatchingException =>
        LOGGER.error(e.getMessage)
        output
      // don't throw exception here.
      case e: Exception =>
        val errorMsg = s"Delete data operation is failed for ${ carbontable.getDatabaseName }.${ tableName }."
        LOGGER.error(errorMsg + e.getMessage)
        throw e
    }
  }

  override protected def opName: String = "UPDATE DATA"
}
