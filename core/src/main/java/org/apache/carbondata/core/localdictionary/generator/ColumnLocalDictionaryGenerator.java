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
package org.apache.carbondata.core.localdictionary.generator;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.localdictionary.dictionaryholder.DictionaryStore;
import org.apache.carbondata.core.localdictionary.dictionaryholder.MapBasedDictionaryStore;

/**
 * Class to generate local dictionary for column
 */
public class ColumnLocalDictionaryGenerator implements LocalDictionaryGenerator {

  /**
   * to check if dictionary size crossed threshold
   */
  private int threshold;

  /**
   * dictionary holder to hold dictionary values
   */
  private DictionaryStore dictionaryHolder;

  public ColumnLocalDictionaryGenerator(int threshold) {
    this.threshold = threshold;
    this.dictionaryHolder = new MapBasedDictionaryStore(threshold);
    // for handling null values
    dictionaryHolder.putIfAbsent(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);
  }

  /**
   * Below method will be used to generate dictionary
   * @param data
   * data for which dictionary needs to be generated
   * @return dictionary value
   */
  @Override public int generateDictionary(byte[] data) {
    int dictionaryValue =  this.dictionaryHolder.putIfAbsent(data);
    return dictionaryValue;
  }

  /**
   * Below method will be used to check if threshold is reached
   * for dictionary for particular column
   * @return true if dictionary threshold reached for column
   */
  @Override public boolean isThresholdReached() {
    return this.threshold < this.dictionaryHolder.size();
  }
}
