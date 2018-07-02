package org.apache.carbondata.core.scan.result.vector.impl;

import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;

public class CarbonDictionaryImpl implements CarbonDictionary {

  private byte[][] dictionary;

  public CarbonDictionaryImpl(byte[][] dictionary) {
    this.dictionary = dictionary;
  }

  @Override public byte[][] getDictionary() {
    return this.dictionary;
  }
}
