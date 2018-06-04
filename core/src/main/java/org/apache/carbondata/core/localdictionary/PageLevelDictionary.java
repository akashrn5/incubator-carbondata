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
package org.apache.carbondata.core.localdictionary;

import java.util.BitSet;

import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;

/**
 * Class to maintain page level dictionary. It will store all unique dictionary values
 * used in a page. This is required while writing blocklet level dictionary in carbondata
 * file
 */
public class PageLevelDictionary {

  /**
   * dictionary generator to generate dictionary values for page data
   */
  private LocalDictionaryGenerator localDictionaryGenerator;

  /**
   * set of dictionary surrogate key in this page
   */
  private BitSet usedDictionaryValues;

  public PageLevelDictionary(LocalDictionaryGenerator localDictionaryGenerator) {
    this.localDictionaryGenerator = localDictionaryGenerator;
    this.usedDictionaryValues = new BitSet();
  }

  /**
   * Below method will be used to get the dictionary value
   *
   * @param data column data
   * @return dictionary value
   * @throws DictionaryThresholdReachedException when threshold crossed for column
   */
  public int getDictionaryValue(byte[] data) throws DictionaryThresholdReachedException {
    if (this.localDictionaryGenerator.isThresholdReached()) {
      throw new DictionaryThresholdReachedException(
          "Unable to generate dictionary value. Dictionary threshold reached");
    }
    int dictionaryValue = localDictionaryGenerator.generateDictionary(data);
    this.usedDictionaryValues.set(dictionaryValue);
    return dictionaryValue;
  }

  /**
   * Method to merge the dictionary value across pages
   *
   * @param pageLevelDictionary other page level dictionary
   */
  public void mergerDictionaryValues(PageLevelDictionary pageLevelDictionary) {
    usedDictionaryValues.and(pageLevelDictionary.usedDictionaryValues);
  }
}
