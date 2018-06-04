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
package org.apache.carbondata.core.datastore.blocklet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.core.datastore.page.key.TablePageKey;

/**
 * Holds the blocklet level data and metadata to be written in carbondata file
 * For dimension pages it will check if all the pages are not encoded with dictionary
 * then it will encode those pages for that column again
 */
public class EncodedBlocklet {

  /**
   * number of rows in a blocklet
   */
  private int blockletSize;

  /**
   * list of page metadata
   */
  private List<TablePageKey> pageMetadataList;

  /**
   * maintains encoded dimension data for each column
   */
  private List<BlockletEncodedColumnPage> encodedDimensionColumnPages;

  /**
   * maintains encoded measure data for each column
   */
  private List<BlockletEncodedColumnPage> encodedMeasureColumnPages;

  /**
   * fallback executor service, will used to re-encode column pages
   */
  private ExecutorService executorService;

  /**
   * number of pages in a blocklet
   */
  private int numberOfPages;

  public EncodedBlocklet(ExecutorService executorService) {
    this.executorService = executorService;
  }

  /**
   * Below method will be used to add page metadata details
   *
   * @param encodedTablePage
   * encoded table page
   */
  private void addPageMetadata(EncodedTablePage encodedTablePage) {
    // for first table page create new list
    if (null == pageMetadataList) {
      pageMetadataList = new ArrayList<>();
    }
    // update details
    blockletSize += encodedTablePage.getPageSize();
    pageMetadataList.add(encodedTablePage.getPageKey());
    this.numberOfPages++;
  }

  /**
   * Below method will be used to add measure column pages
   *
   * @param encodedTablePage
   * encoded table page
   * @throws ExecutionException
   * in case of any failure in adding
   * @throws InterruptedException
   * in case of any failure in adding
   */
  private void addEncodedMeasurePage(EncodedTablePage encodedTablePage)
      throws ExecutionException, InterruptedException {
    // for first page create new list
    if (null == encodedMeasureColumnPages) {
      encodedMeasureColumnPages = new ArrayList<>();
      // adding measure pages
      for (int i = 0; i < encodedTablePage.getNumMeasures(); i++) {
        encodedMeasureColumnPages
            .add(new BlockletEncodedColumnPage(null, encodedTablePage.getMeasure(i)));
      }
    } else {
      for (int i = 0; i < encodedTablePage.getNumMeasures(); i++) {
        encodedMeasureColumnPages.get(i).addEncodedColumnColumnPage(encodedTablePage.getMeasure(i));
      }
    }
  }

  /**
   * Below method will be used to add dimension column pages
   *
   * @param encodedTablePage
   * encoded table page
   * @throws ExecutionException
   * in case of any failure in adding
   * @throws InterruptedException
   * in case of any failure in adding
   */
  private void addEncodedDimensionPage(EncodedTablePage encodedTablePage)
      throws ExecutionException, InterruptedException {
    // for first page create new list
    if (null == encodedDimensionColumnPages) {
      encodedDimensionColumnPages = new ArrayList<>();
      // adding measure pages
      for (int i = 0; i < encodedTablePage.getNumDimensions(); i++) {
        encodedDimensionColumnPages
            .add(new BlockletEncodedColumnPage(executorService, encodedTablePage.getDimension(i)));
      }
    } else {
      for (int i = 0; i < encodedTablePage.getNumDimensions(); i++) {
        encodedDimensionColumnPages.get(i)
            .addEncodedColumnColumnPage(encodedTablePage.getDimension(i));
      }
    }
  }

  /**
   * Use to add table pages
   *
   * @param encodedTablePage
   * encoded table page
   * @throws ExecutionException
   * in case of any failure while adding
   * @throws InterruptedException
   * in case of any failure while adding
   */
  public void addEncodedTablePage(EncodedTablePage encodedTablePage)
      throws ExecutionException, InterruptedException {
    addPageMetadata(encodedTablePage);
    addEncodedDimensionPage(encodedTablePage);
    addEncodedMeasurePage(encodedTablePage);
  }

  public int getBlockletSize() {
    return blockletSize;
  }

  public List<TablePageKey> getPageMetadataList() {
    return pageMetadataList;
  }

  public List<BlockletEncodedColumnPage> getEncodedDimensionColumnPages() {
    return encodedDimensionColumnPages;
  }

  public List<BlockletEncodedColumnPage> getEncodedMeasureColumnPages() {
    return encodedMeasureColumnPages;
  }

  public int getNumberOfDimension() {
    return encodedDimensionColumnPages.size();
  }

  public int getNumberOfMeasure() {
    return encodedMeasureColumnPages.size();
  }

  public int getNumberOfPages() {
    return this.numberOfPages;
  }

  public void clear() {
    this.numberOfPages = 0;
    this.encodedDimensionColumnPages = null;
    this.blockletSize = 0;
    this.encodedMeasureColumnPages = null;
    this.pageMetadataList = null;
  }
}
