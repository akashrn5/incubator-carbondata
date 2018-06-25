package org.apache.carbondata.core.localdictionary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodingFactory;
import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;
import org.apache.carbondata.core.localdictionary.generator.ColumnLocalDictionaryGenerator;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.LocalDictionaryChunk;

import org.junit.Assert;
import org.junit.Test;

public class TestPageLevelDictionary {

  @Test public void testPageLevelDictionaryGenerateDataIsGeneratingProperDictionaryValues() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000);
    String columnName = "column1";
    PageLevelDictionary pageLevelDictionary = new PageLevelDictionary(generator, columnName);
    try {
      for (int i = 1; i <= 1000; i++) {
        Assert.assertTrue((i + 1) == pageLevelDictionary.getDictionaryValue(("" + i).getBytes()));
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
  }

  @Test public void testPageLevelDictionaryContainsOnlyUsedDictionaryValues() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000);
    String columnName = "column1";
    PageLevelDictionary pageLevelDictionary1 = new PageLevelDictionary(generator, columnName);
    byte[][] validateData = new byte[500][];
    try {
      for (int i = 1; i <= 500; i++) {
        validateData[i - 1] = ("vishal" + i).getBytes();
        pageLevelDictionary1.getDictionaryValue(validateData[i - 1]);
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    PageLevelDictionary pageLevelDictionary2 = new PageLevelDictionary(generator, columnName);
    try {
      for (int i = 1; i <= 500; i++) {
        pageLevelDictionary2.getDictionaryValue(("vikas" + i).getBytes());
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    try {
      LocalDictionaryChunk localDictionaryChunkForBlocklet =
          pageLevelDictionary1.getLocalDictionaryChunkForBlocklet();
      List<Encoding> encodings = localDictionaryChunkForBlocklet.getDictionary_meta().getEncoders();
      EncodingFactory encodingFactory = DefaultEncodingFactory.getInstance();
      List<ByteBuffer> encoderMetas =
          localDictionaryChunkForBlocklet.getDictionary_meta().getEncoder_meta();
      ColumnPageDecoder decoder = encodingFactory.createDecoder(encodings, encoderMetas);
      ColumnPage decode = decoder.decode(localDictionaryChunkForBlocklet.getDictionary_data(), 0,
          localDictionaryChunkForBlocklet.getDictionary_data().length);
      for (int i = 0; i < 500; i++) {
        Arrays.equals(decode.getBytes(i), validateData[i]);
      }
    } catch (MemoryException e) {
      Assert.assertTrue(false);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testPageLevelDictionaryContainsOnlyUsedDictionaryValuesWhenMultiplePagesUseSameDictionary() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000);
    String columnName = "column1";
    PageLevelDictionary pageLevelDictionary1 = new PageLevelDictionary(generator, columnName);
    byte[][] validateData = new byte[20][];
    int index = 0;
    try {
      for (int i = 1; i <= 5; i++) {
        validateData[index] = ("vishal" + i).getBytes();
        pageLevelDictionary1.getDictionaryValue(validateData[index]);
        index++;
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    PageLevelDictionary pageLevelDictionary2 = new PageLevelDictionary(generator, columnName);
    try {
      for (int i = 1; i <= 5; i++) {
        validateData[index++] = CarbonCommonConstants.EMPTY_BYTE_ARRAY;
        pageLevelDictionary2.getDictionaryValue(("vikas" + i).getBytes());
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    try {
      for (int i = 6; i <= 10; i++) {
        validateData[index] = ("vishal" + i).getBytes();
        pageLevelDictionary1.getDictionaryValue(validateData[index]);
        index++;
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    try {
      for (int i = 6; i <= 10; i++) {
        validateData[index++] = CarbonCommonConstants.EMPTY_BYTE_ARRAY;
        pageLevelDictionary2.getDictionaryValue(("vikas" + i).getBytes());
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    try {
      LocalDictionaryChunk localDictionaryChunkForBlocklet =
          pageLevelDictionary1.getLocalDictionaryChunkForBlocklet();
      List<Encoding> encodings = localDictionaryChunkForBlocklet.getDictionary_meta().getEncoders();
      EncodingFactory encodingFactory = DefaultEncodingFactory.getInstance();
      List<ByteBuffer> encoderMetas =
          localDictionaryChunkForBlocklet.getDictionary_meta().getEncoder_meta();
      ColumnPageDecoder decoder = encodingFactory.createDecoder(encodings, encoderMetas);
      ColumnPage decode = decoder.decode(localDictionaryChunkForBlocklet.getDictionary_data(), 0,
          localDictionaryChunkForBlocklet.getDictionary_data().length);
      for (int i = 0; i < 15; i++) {
        Arrays.equals(decode.getBytes(i), validateData[i]);
      }
    } catch (MemoryException e) {
      Assert.assertTrue(false);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
  }
}
