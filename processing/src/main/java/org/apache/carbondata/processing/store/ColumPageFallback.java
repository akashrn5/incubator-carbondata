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
package org.apache.carbondata.processing.store;

import java.util.concurrent.Callable;

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodingFactory;

/**
 * Below class will be used to encode column pages for which local dictionary was generated
 * but all the pages in blocklet was not encoded with local dictionary.
 * This is required as all the pages of a column in blocklet either it will be local dictionary
 * encoded or decoded.
 */
public class ColumPageFallback implements Callable<EncodedColumnPage> {

  /**
   * actual local dictionary generated column page
   */
  private EncodedColumnPage actualColumnPage;

  /**
   * encoder factory
   */
  private EncodingFactory encodingFactory = DefaultEncodingFactory.getInstance();

  public ColumPageFallback(EncodedColumnPage actualColumnPage) {
    this.actualColumnPage = actualColumnPage;
  }

  @Override public EncodedColumnPage call() throws Exception {
    // disbale encoding using local dictionary
    actualColumnPage.getActualPage().disableLocalDictEncoding();
    // get new encoder
    ColumnPageEncoder columnPageEncoder = encodingFactory.createEncoder(
        actualColumnPage.getActualPage().getColumnSpec(),
        actualColumnPage.getActualPage());
    // encode column page
    return columnPageEncoder.encode(actualColumnPage.getActualPage());
  }
}
