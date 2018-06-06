package org.apache.carbondata.spark.testsuite.localdictionary

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class LocalDictionarySupportTest extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS LOCAL1")
    sql("DROP TABLE IF EXISTS LOCAL2")
    sql("DROP TABLE IF EXISTS LOCAL3")
    sql("DROP TABLE IF EXISTS LOCAL4")
    sql("DROP TABLE IF EXISTS LOCAL5")
    sql("DROP TABLE IF EXISTS LOCAL6")
    sql("DROP TABLE IF EXISTS LOCAL7")
    sql("DROP TABLE IF EXISTS LOCAL8")
  }

  test("test local dictionary default configuration") {
    sql(
      """
        | CREATE TABLE local1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    val desc_result = sql("describe formatted local1")

    val descLoc = sql("describe formatted local1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Support")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("1000"))
    }
  }

  test("test local dictionary custom configurations") {
    sql(
      """
        | CREATE TABLE local2(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='10000')
      """.stripMargin)

    val descLoc = sql("describe formatted local2").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Support")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }
  }

  test("test local dictionary invalid configurations") {
    sql(
      """
        | CREATE TABLE local3(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false')
      """.stripMargin)

    val descLoc1 = sql("describe formatted local3").collect
    descLoc1.find(_.get(0).toString.contains("Local Dictionary Support")) match {
      case Some(row) => assert(row.get(1).toString.contains("false"))
    }
    val locThreshold = descLoc1.find(_.get(0).toString.contains("Local Dictionary Threshold"))
    assert(locThreshold.isEmpty)

    sql("drop table if exists local3")
    sql(
      """
        | CREATE TABLE local3(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_threshold'='10000')
      """.stripMargin)
    val descLoc = sql("describe formatted local3").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Support")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
    }

  }

  test ("test set local dictionary threshold using alter table set command") {
    sql(
      """
        | CREATE TABLE local4(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    val local_dict_result = sql("describe formatted local4")

    checkExistence(local_dict_result, true, "Local Dictionary Support")
    checkExistence(local_dict_result, true, "Local Dictionary Threshold")
    checkExistence(local_dict_result, false, "10000")

    sql(
      s"""
         | alter table local4
         | SET TBLPROPERTIES (
         | 'local_dict_threshold'='10000'
         | )
       """.stripMargin)

    val local_dict_result_alter = sql("describe formatted local4")
    checkExistence(local_dict_result_alter, true, "10000")
  }

  test("test to validate local dict columns") {
    val exception =intercept[MalformedCarbonCommandException] {
      sql(
        """
        | CREATE TABLE local5(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_columns'='')
      """.
          stripMargin)
      }
    assert(exception.getMessage.contains("LOCAL_DICT_COLUMNS column:  does not exist in table. Please check create table statement"))
    sql("drop table if exists local5")
    val exception1 = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local5(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_columns'='abc')
        """.
          stripMargin)
    }
    assert(exception1.getMessage.contains("LOCAL_DICT_COLUMNS column: abc does not exist in table. Please check create table statement"))
  }

  test("test to validate local dict columns for invalid datatype columns") {
    val exception =intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local6(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('local_dict_columns'='id')
        """.
          stripMargin)
    }
    assert(exception.getMessage.contains("LOCAL_DICT_COLUMNS column: id is not a String datatype column. LOCAL_DICT_COLUMN should be no dictionary string datatype column"))
  }

  test("test to validate local dict columns present in dictionary include columns") {
    val exception =intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE local7(id int, name string, city string, age int)
          | STORED BY 'org.apache.carbondata.format'
          | tblproperties('dictionary_include'='name','local_dict_columns'='name')
        """.
          stripMargin)
    }
    assert(exception.getMessage.contains("LOCAL_DICT_COLUMNS column: name does exist as a DICTIONARY_INCLUDE column. LOCAL_DICT_COLUMNS should be no dictionary string datatype columns"))
  }

  test("test describe formatted for local dict columns when local dict is enabled and disabled") {
    sql(
      """
        | CREATE TABLE local8(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_columns'='name')
      """.
        stripMargin)
    val descFormatted1 = sql("describe formatted local8")
    checkExistence(descFormatted1, true, "Local Dictionary Columns")

    sql("drop table if exists local8")
    sql(
      """
        | CREATE TABLE local8(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
        | tblproperties('local_dict_enable'='false','local_dict_columns'='name')
      """.
        stripMargin)
    val descFormatted2 = sql("describe formatted local8")
    checkExistence(descFormatted2, false, "Local Dictionary Columns")
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS LOCAL1")
    sql("DROP TABLE IF EXISTS LOCAL2")
    sql("DROP TABLE IF EXISTS LOCAL3")
    sql("DROP TABLE IF EXISTS LOCAL4")
    sql("DROP TABLE IF EXISTS LOCAL5")
    sql("DROP TABLE IF EXISTS LOCAL6")
    sql("DROP TABLE IF EXISTS LOCAL7")
    sql("DROP TABLE IF EXISTS LOCAL8")
  }
}
