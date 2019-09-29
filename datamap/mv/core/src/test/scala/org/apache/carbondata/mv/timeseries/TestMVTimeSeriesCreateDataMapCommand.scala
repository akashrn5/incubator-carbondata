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

package org.apache.carbondata.mv.timeseries

import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.mv.rewrite.TestUtil

class TestMVTimeSeriesCreateDataMapCommand extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql("drop table if exists products")
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS maintable")
  }

  test("test mv_timeseries create datamap") {
   createFactTable("maintable")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 on table maintable using 'mv_timeseries'" +
      " as select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    sql("show datamap on table maintable").show(false)
    loadDataToFactTable("maintable")
    val result = sql("show datamap on table maintable").collectAsList()
    assert(result.get(0).get(0).toString.equalsIgnoreCase("datamap1"))
    assert(result.get(0).get(4).toString.equalsIgnoreCase("ENABLED"))
    sql("select * from datamap1_table").show(false)
    sql("select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')").show(false)
    sql("explain select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')").show(false)
    sql("drop datamap if exists datamap1")
    sql("drop table IF EXISTS maintable")
  }

  test("test mv_timeseries create lazy datamap") {
   createFactTable("maintable")
    sql("drop datamap if exists datamap1")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create datamap datamap1 on table maintable using 'mv_timeseries' with deferred rebuild as " +
        "select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    }.getMessage.contains("MV TimeSeries datamap does not support Lazy Rebuild")
    sql("drop table IF EXISTS maintable")
  }

  test("test mv timeseries with join queries") {
    sql("drop table if exists products")
    sql("create table products (product string, date timestamp) stored by 'carbondata' ")
    intercept[MalformedCarbonCommandException] {
      sql(
        "Create datamap innerjoin on table products using 'mv_timeseries' as Select p.product, p.date, " +
        "s.product, s.date from products p, products s where p.product=s.product")
    }.getMessage.contains("Cannot create MV Timeseries datamap for queries involving more than " +
                          "one parent table or join queries on same table")
    sql("drop table if exists products")
  }

  test("test mv_timeseries create datamap with multiple granularity") {
   createFactTable("maintable")
    sql("drop datamap if exists datamap1")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create datamap datamap1 on table maintable using 'mv_timeseries'  as " +
        "select timeseries(projectjoindate,'second'), timeseries(projectjoindate,'hour') from maintable")
    }.getMessage.contains("Multiple timeseries udf functions are defined in Select statement with different granularities")
    sql("drop table IF EXISTS maintable")
  }

  test("test mv_timeseries create datamap with date type as timeseries_column") {
    sql("drop table IF EXISTS maintable")
    sql("CREATE TABLE maintable (projectcode int, projectjoindate date, projectenddate Timestamp,attendance int) " +
        "STORED BY 'org.apache.carbondata.format'")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 on table maintable using 'mv_timeseries' as " +
      "select timeseries(projectjoindate,'day') from maintable")
    sql("drop table IF EXISTS maintable")
  }

  test("test mv_timeseries create datamap with date type as timeseries_column with incorrect granularity") {
    sql("drop table IF EXISTS maintable")
    sql("CREATE TABLE maintable (projectcode int, projectjoindate date, projectenddate Timestamp,attendance int) " +
        "STORED BY 'org.apache.carbondata.format'")
    sql("drop datamap if exists datamap1")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create datamap datamap1 on table maintable using 'mv_timeseries' as " +
        "select timeseries(projectjoindate,'second') from maintable")
    }.getMessage
      .contains("Granularity should be DAY,MONTH or YEAR, for timeseries column of Date type")
    sql("drop table IF EXISTS maintable")
  }

  test("test mv_timeseries create datamap - Parent table name is different in Create and Select Statement") {
    createFactTable("maintable")
    createFactTable("main_table")
    sql("drop datamap if exists datamap1")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create datamap datamap1 on table main_table using 'mv_timeseries' as " +
        "select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    }.getMessage.contains("Parent table name is different in Create and Select Statement")
    sql("drop datamap if exists datamap1")
    sql("drop table IF EXISTS maintable")
    sql("drop table IF EXISTS main_table")
  }

  test("test mv_timeseries for same event_column with different granularities") {
   createFactTable("maintable")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 on table maintable using 'mv_timeseries' as " +
      "select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    sql("drop datamap if exists datamap2")
    sql(
      "create datamap datamap2 on table maintable using 'mv_timeseries' as " +
      "select timeseries(projectjoindate,'hour'), sum(projectcode) from maintable group by timeseries(projectjoindate,'hour')")
    sql("drop datamap if exists datamap3")
    sql(
      "create datamap datamap3 on table maintable using 'mv_timeseries' as " +
      "select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable group by timeseries(projectjoindate,'minute')")
    sql("drop datamap if exists datamap4")
    sql(
      "create datamap datamap4 on table maintable using 'mv_timeseries' as " +
      "select timeseries(projectjoindate,'day'), sum(projectcode) from maintable group by timeseries(projectjoindate,'day')")
    sql("drop datamap if exists datamap5")
    sql(
      "create datamap datamap5 on table maintable using 'mv_timeseries' as " +
      "select timeseries(projectjoindate,'year'), sum(projectcode) from maintable group by timeseries(projectjoindate,'year')")
    sql("drop table IF EXISTS main_table")
  }

  test("test mv_timeseries create datamap with more event_columns") {
   createFactTable("maintable")
    sql("drop datamap if exists datamap1")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create datamap datamap1 on table maintable using 'mv_timeseries' as " +
        "select timeseries(projectjoindate,'hour'), timeseries(projectenddate,'hour') from maintable")
    }.getMessage.contains(
        "Multiple timeseries udf functions are defined in Select statement with different timestamp columns")
    sql("drop table IF EXISTS main_table")
  }

  test("test mv_timeseries create datamap without timeseries udf") {
   createFactTable("maintable")
    sql("drop datamap if exists datamap1")
    intercept[MalformedCarbonCommandException] {
      sql("create datamap datamap1 on table maintable using 'mv_timeseries' as " +
          "select projectjoindate, sum(projectcode) from maintable group by projectjoindate")
    }.getMessage.contains("TimeSeries UDF has to be defined in Select statement for using MV Timeseries datamap")
    sql("drop table IF EXISTS main_table")
  }

  test("test mv_timeseries create datamap with same granularity and different ctas") {
   createFactTable("maintable")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 on table maintable using 'mv_timeseries' as " +
      "select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    sql("drop datamap if exists datamap2")
    sql(
      "create datamap datamap2 on table maintable using 'mv_timeseries' as " +
      "select timeseries(projectjoindate,'second'), sum(projectcode) from maintable where projectjoindate='29-06-2008 00:00:00.0' " +
      "group by timeseries(projectjoindate,'second')")
    sql("drop table IF EXISTS main_table")
  }

  test("insert and create datamap in progress") {
    sql("drop table IF EXISTS maintable")
    createFactTable("maintable")
    sql("drop datamap if exists datamap1")
    val query = s"LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE maintable  " +
                s"OPTIONS('DELIMITER'= ',')"
    val executorService = Executors.newFixedThreadPool(4)
    executorService.submit(new QueryTask(query))
    intercept[UnsupportedOperationException] {
      sql(
        "create datamap datamap1 on table maintable using 'mv_timeseries' as " +
        "select timeseries(projectjoindate,'year'), sum(projectcode) from maintable group by timeseries(projectjoindate,'year')")
    }.getMessage
      .contains("Cannot create mv datamap table when insert is in progress on parent table: maintable")
    executorService.shutdown()
    executorService.awaitTermination(2, TimeUnit.HOURS)
    sql("drop table IF EXISTS maintable")
  }

  test("test load data and create datamap") {
    createFactTable("maintable")
    loadDataToFactTable("maintable")
    sql("drop datamap if exists datamap2")
    sql(
      "create datamap datamap2 on table maintable using 'mv_timeseries' as " +
      "select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable group by timeseries(projectjoindate,'minute')")
    assert(sql("select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable group by timeseries(projectjoindate,'minute')").count() == 10)
    val df1 = sql("select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable group by timeseries(projectjoindate,'minute')")
    val analyzed1 = df1.queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "datamap2"))
  }

  test("test create datamap with incorrect timeseries_column and granularity") {
    createFactTable("maintable")
    loadDataToFactTable("maintable")
    sql("drop datamap if exists datamap2")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create datamap datamap2 on table maintable using 'mv_timeseries' as " +
        "select timeseries(projectjoindate,'time'), sum(projectcode) from maintable group by timeseries(projectjoindate,'time')")
    }.getMessage.contains("Granularity time is invalid")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create datamap datamap2 on table maintable using 'mv_timeseries' as " +
        "select timeseries(empname,'second'), sum(projectcode) from maintable group by timeseries(empname,'second')")
    }.getMessage.contains("MV Timeseries is only supported on Timestamp/Date column")
  }

  class QueryTask(query: String) extends Callable[String] {
    override def call(): String = {
      var result = "PASS"
      try {
        sql(query).collect()
      } catch {
        case exception: Exception => LOGGER.error(exception.getMessage)
          result = "FAIL"
      }
      result
    }
  }

  override def afterAll(): Unit = {
    sql("drop table if exists products")
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS maintable")
  }

  private def createFactTable(tableName: String) = {
    sql(s"drop table IF EXISTS $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName (empname String, designation String, doj Timestamp,
         |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
         |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
         |  utilization int,salary int)
         | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
  }

  private def loadDataToFactTable(tableName: String) = {
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE $tableName  OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
  }

}
