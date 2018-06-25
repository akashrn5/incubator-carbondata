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
package org.apache.carbondata.spark.testsuite.localdictionary;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.spark.sql.test.util.QueryTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class to test local dictionary generation
 */
public class LocalDictionarySupportLoadTableTest extends QueryTest {

  String file1 = resourcesPath() + "/local_dictionary_source1.csv";

  String file2 = resourcesPath() + "/local_dictionary_complex_data.csv";

  String storePath = warehouse() + "/local2/Fact/Part0/Segment_0";

  @Before
  public void dropTable() throws FileNotFoundException {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.BLOCKLET_SIZE, "10000");
    createFile(file1);
    createFileForComplexData(file2);
    sql("drop table if exists local2");
  }

  @After
  public void dropTableAfter() {
    deleteFile(file1);
    deleteFile(file2);
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.BLOCKLET_SIZE,
        CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
  }

  @Test
  public void testLocalDictionaryLoadForFallBackScenario() throws IOException {
    sql("drop table if exists local2");
    sql("CREATE TABLE local2(name string) STORED BY 'carbondata' tblproperties('local_dictionary_threshold'='2001','local_dictionary_include'='name')");
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')");
    Assert.assertFalse(checkForLocalDictionary());
  }

  @Test
  public void testSuccessfulLocalDictionaryGeneration() throws IOException {
    sql("drop table if exists local2");
    sql("CREATE TABLE local2(name string) STORED BY 'carbondata' tblproperties('local_dictionary_threshold'='9001','local_dictionary_include'='name')");
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')");
    Assert.assertTrue(checkForLocalDictionary());
  }

  @Test
  public void testSuccessfulLocalDictionaryGenerationOnDefaultConfigs() throws IOException {
    sql("drop table if exists local2");
    sql("CREATE TABLE local2(name string) STORED BY 'carbondata'");
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')");
    Assert.assertTrue(checkForLocalDictionary());
  }

  @Test
  public void testLocalDictioanryGenerationForDictionayIncludeColumn() throws IOException {
    sql("drop table if exists local2");
    sql("CREATE TABLE local2(name string) STORED BY 'carbondata' tblproperties('dictionary_include'='name')");
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')");
    Assert.assertFalse(checkForLocalDictionary());
  }

  @Test
  public void testLocalDictioanryGenerationForDictionayExcludeColumn() throws IOException {
    sql("drop table if exists local2");
    sql("CREATE TABLE local2(name string) STORED BY 'carbondata' tblproperties('dictionary_exclude'='name')");
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')");
    Assert.assertTrue(checkForLocalDictionary());
  }

  @Test
  public void testLocalDictioanryGenerationForLocalDictionayExcludeColumn()
      throws IOException {
    sql("drop table if exists local2");
    sql("CREATE TABLE local2(name string) STORED BY 'carbondata' tblproperties('local_dictionary_exclude'='name')");
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')");
    Assert.assertFalse(checkForLocalDictionary());
  }

  @Test
  public void testLocalDictioanryGenerationWhenItIsDisabled() throws IOException {
    sql("drop table if exists local2");
    sql("CREATE TABLE local2(name string) STORED BY 'carbondata' tblproperties('local_dictionary_enable'='false','local_dictionary_include'='name')");
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')");
    Assert.assertFalse(checkForLocalDictionary());
  }

  @Test
  public void testLocalDictioanryGenerationForInvalidThreshold() throws IOException {
    sql("drop table if exists local2");
    sql("CREATE TABLE local2(name string) STORED BY 'carbondata' tblproperties('local_dictionary_include'='name','local_dictionary_threshold'='300000')");
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')");
    Assert.assertTrue(checkForLocalDictionary());
  }

  @Test
  public void testLocalDictioanryGenerationForComplexType() throws IOException {
    sql("drop table if exists local2");
    sql("CREATE TABLE local2(name struct<i:string,s:string>) STORED BY 'carbondata' tblproperties('local_dictionary_include'='name')");
    sql("load data inpath '" + file2 + "' into table local2 OPTIONS('header'='false','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')");
    Assert.assertTrue(checkForLocalDictionary());
  }

  /**
   * this method returns true if local dictionary is created for all the blocklets or not
   *
   * @return
   * @throws IOException
   */
  public boolean checkForLocalDictionary() throws IOException {
    boolean isLocalDictionaryGenerated = false;
    CarbonFile[] dataFiles = FileFactory.getCarbonFile(storePath).listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
          return true;
        } else {
          return false;
        }
      }
    });
    List<DimensionRawColumnChunk> dimensionRawColumnChunks =
        CarbonUtil.read(dataFiles[0].getAbsolutePath());
    for (DimensionRawColumnChunk dimensionRawColumnChunk : dimensionRawColumnChunks) {
      if (dimensionRawColumnChunk.getDataChunkV3().isSetLocal_dictionary()) {
        isLocalDictionaryGenerated = true;
      } else {
        isLocalDictionaryGenerated = false;
      }
    }
    return isLocalDictionaryGenerated;
  }

  /**
   * create the csv data file
   * @param fileName
   * @throws FileNotFoundException
   */
  public void createFile(String fileName) throws FileNotFoundException {
    int line = 9000;
    int start = 0;
    ArrayList<String> data = new ArrayList<>();
    for (int i = start; i < (line + start); i++) {
      data.add("n" + i);
    }
    Collections.sort(data);
    PrintWriter writer = new PrintWriter(new File(fileName));
    for (String eachData : data) {
      writer.println(eachData);
    }
    writer.close();
  }

  /**
   * create the csv file for complex column data
   * @param fileName
   * @throws FileNotFoundException
   */
  public void createFileForComplexData(String fileName) throws FileNotFoundException {
    int line = 9000;
    int start = 0;
    ArrayList<String> data = new ArrayList<>();
    for (int i = start; i < (line + start); i++) {
      data.add("n" + i + "$" + (i+1));
    }
    Collections.sort(data);
    PrintWriter writer = new PrintWriter(new File(fileName));
    for (String eachData : data) {
      writer.println(eachData);
    }
    writer.close();
  }

  /**
   * delete csv file after test execution
   * @param fileName
   */
  public void deleteFile(String fileName) {
    File file = new File(fileName);
    if (file.exists()) {
      file.delete();
    }
  }
}
