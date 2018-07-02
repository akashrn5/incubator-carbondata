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
