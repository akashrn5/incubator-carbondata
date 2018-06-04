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

package org.apache.carbondata.core.datastore.page;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.carbondata.core.localdictionary.PageLevelDictionary;
import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Column page implementation for Local dictionary generated columns
 * Its a decorator over two column page
 * 1. Which will hold the actual data
 * 2. Which will hold the dictionary encoded data
 *
 */
public class LocalDictColumnPage extends ColumnPage{

  /**
   * to maintain page level dictionary for column page
   */
  private PageLevelDictionary pageLevelDictionary;

  /**
   * to hold the actual data of the column
   */
  private ColumnPage actualDataColumnPage;

  /**
   * to hold the dictionary encoded column page
   */
  private ColumnPage encodedDataColumnPage;

  /**
   * Create a new column page with input data type and page size.
   */
  protected LocalDictColumnPage(ColumnPage actualDataColumnPage, ColumnPage encodedColumnpage,
      LocalDictionaryGenerator localDictionaryGenerator) {
    super(actualDataColumnPage.getColumnSpec(), actualDataColumnPage.getDataType(),
        actualDataColumnPage.getPageSize());
    pageLevelDictionary = new PageLevelDictionary(localDictionaryGenerator);
    this.actualDataColumnPage = actualDataColumnPage;
    this.encodedDataColumnPage = encodedColumnpage;
  }

  @Override public byte[][] getByteArrayPage() {
    if(null != pageLevelDictionary) {
      return encodedDataColumnPage.getByteArrayPage();
    } else {
      return actualDataColumnPage.getByteArrayPage();
    }
  }

  /**
   * Below method will be used to check whether page is local dictionary
   * generated or not. This will be used for while enoding the the page
   * @return
   */
  public boolean isLocalDictGeneratedPage() {
    return null != pageLevelDictionary;
  }

  /**
   * Below method will be used to add column data to page
   * @param rowId
   * row number
   * @param bytes
   * actual data
   */
  @Override public void putBytes(int rowId, byte[] bytes) {
    if (null != pageLevelDictionary) {
      try {
        int dictionaryValue = pageLevelDictionary.getDictionaryValue(bytes);
        actualDataColumnPage.putBytes(rowId, bytes);
        encodedDataColumnPage.putBytes(rowId, ByteUtil.toBytes(dictionaryValue));
      } catch (DictionaryThresholdReachedException e) {
        pageLevelDictionary = null;
        encodedDataColumnPage = null;
      }
    } else {
      actualDataColumnPage.putBytes(rowId, bytes);
    }
  }

  public PageLevelDictionary getPageDictionary() {
    return pageLevelDictionary;
  }

  @Override public void disableLocalDictEncoding() {
    pageLevelDictionary = null;
  }


  @Override public PageLevelDictionary getColumnPageDictionary() {
    return pageLevelDictionary;
  }

  @Override public void setBytePage(byte[] byteData) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void setShortPage(short[] shortData) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void setShortIntPage(byte[] shortIntData) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void setIntPage(int[] intData) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void setLongPage(long[] longData) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void setFloatPage(float[] floatData) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void setDoublePage(double[] doubleData) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void setByteArrayPage(byte[][] byteArray) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void freeMemory() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void putByte(int rowId, byte value) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void putShort(int rowId, short value) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void putInt(int rowId, int value) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void putLong(int rowId, long value) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void putDouble(int rowId, double value) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void putDecimal(int rowId, BigDecimal decimal) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void putShortInt(int rowId, int value) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public byte getByte(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public short getShort(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public int getShortInt(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public int getInt(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public long getLong(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public float getFloat(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public double getDouble(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public byte[] getBytes(int rowId) {
    return actualDataColumnPage.getBytes(rowId);
  }

  @Override public byte[] getBytePage() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public short[] getShortPage() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public byte[] getShortIntPage() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public int[] getIntPage() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public long[] getLongPage() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public float[] getFloatPage() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public double[] getDoublePage() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public byte[] getLVFlattenedBytePage() throws IOException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public byte[] getComplexChildrenLVFlattenedBytePage() throws IOException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public byte[] getComplexParentFlattenedBytePage() throws IOException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public byte[] getDecimalPage() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void convertValue(ColumnPageValueConverter codec) {
    throw new UnsupportedOperationException("Operation not supported");
  }
}
