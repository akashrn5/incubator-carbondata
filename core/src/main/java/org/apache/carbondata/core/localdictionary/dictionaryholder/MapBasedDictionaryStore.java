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
package org.apache.carbondata.core.localdictionary.dictionaryholder;

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.cache.dictionary.DictionaryByteArrayWrapper;

/**
 * Map based dictionary holder class, it will use map to hold
 * the dictionary key and its value
 */
public class MapBasedDictionaryStore implements DictionaryStore {

  /**
   * use to assign dictionary value to new key
   */
  private int lastAssignValue;

  /**
   * to maintain dictionary key value
   */
  private Map<DictionaryByteArrayWrapper, Integer> dictionary;

  /**
   * maintaining array for reverse lookup
   * otherwise iterating everytime in map for reverse lookup will be slowdown the performance
   * It will only maintain the reference
   */
  private byte[][] assignKey;

  public MapBasedDictionaryStore(int intialSize) {
    this.dictionary = new HashMap<>();
    this.assignKey = new byte[intialSize][];
  }

  /**
   * Below method will be used to add dictionary value to dictionary holder
   * if it is already present in the holder then it will return exiting dictionary value.
   * @param data
   * dictionary key
   * @return dictionary value
   */
  @Override public int putIfAbsent(byte[] data) {
    DictionaryByteArrayWrapper key = new DictionaryByteArrayWrapper(data);
    Integer value = dictionary.get(key);
    if(null == value) {
      synchronized (dictionary) {
        value = dictionary.get(key);
        if(null == value) {
          value = ++lastAssignValue;
          this.assignKey[value.intValue()] = data;
          dictionary.put(key, value);
        }
      }
    }
    return value;

  }

  /**
   * Below method to get the current size of dictionary
   * @return
   */
  @Override public int size() {
    return dictionary.size();
  }

  /**
   * Below method will be used to get the dictionary key based on value
   * @param value
   * dictionary value
   * Caller will take of passing proper value
   * @return dictionary key based on value
   */
  @Override public byte[] getDictionaryKeyBasedOnValue(int value) {
    return assignKey[value];
  }
}
