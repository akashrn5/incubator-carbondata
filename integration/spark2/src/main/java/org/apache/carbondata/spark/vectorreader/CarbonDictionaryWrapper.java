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
package org.apache.carbondata.spark.vectorreader;

import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;

public class CarbonDictionaryWrapper extends Dictionary {

  private Binary[] binaries;

  public CarbonDictionaryWrapper(Encoding encoding, CarbonDictionary dictionary) {
    super(encoding);
    byte[][] rleData = dictionary.getDictionary();
    if (rleData != null) {
      binaries = new Binary[rleData.length];
      for (int i = 0; i < rleData.length; i++) {
        binaries[i] = Binary.fromReusedByteArray(rleData[i]);
      }
    }
  }

  @Override public int getMaxId() {
    return 0;
  }

  @Override public Binary decodeToBinary(int id) {
    return binaries[id];
  }
}
