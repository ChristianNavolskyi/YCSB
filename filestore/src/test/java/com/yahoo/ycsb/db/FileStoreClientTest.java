/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import org.junit.*;

import java.io.File;
import java.util.*;

public class FileStoreClientTest {

  private static final String RECORD_COUNT_IDENTIFIER = "recordcount";
  private static final int RECORD_COUNT = 1000;

  private static final String OPERATION_COUNT_IDENTIFIER = "operationcount";
  private static final int OPERATION_COUNT = 1000;

  private static final String READ_PROPORTION_IDENTIFIER = "readproportion";
  private static final double READ_PROPORTION = 0.1;

  private static final String UPDATE_PROPORTION_IDENTIFIER = "updateidentifier";
  private static final double UPDATE_PROPORTION = 0.1;

  private static final String SCAN_PROPORTION_IDENTIFIER = "scanproportion";
  private static final double SCAN_PROPORTION = 0.1;

  private static final String INSERT_PROPORTION_IDENTIFIER = "insertproportion";
  private static final double INSERT_PROPORTION = 0.7;

  private static final String REQUEST_DISTRIBUTION_IDENTIFIER = "requestdistribution";
  private static final String REQUEST_DISTRIBUTION = "zipifan";

  private static final String OUTPUT_DIRECTORY_IDENTIFIER = "outputDirectory";
  private static final String OUTPUT_DIRECTORY = System.getProperty("user.dir") + File.separatorChar + "tmp" + File
      .separatorChar;

  private static final String ENABLE_PRETTY_PRINTING_IDENTIFIER = "enablePrettyPrinting";
  private static final String ENABLE_PRETTY_PRINTING = "true";

  private static final String TABLE = "table";
  private static final String KEY = "key";
  private static final String MAP_KEY = "mapKey";
  private static final String ITERATOR_VALUE = "test";
  private static final String OTHER_ITERATOR_VALUE = "other Value";
  private static final String OTHER_MAP_KEY = "other";

  private static Properties workloadProperties;
  private FileStoreClient fileStoreClient;

  @BeforeClass
  public static void setUpWorkload() {
    workloadProperties = new Properties();
    workloadProperties.setProperty(RECORD_COUNT_IDENTIFIER, String.valueOf(RECORD_COUNT));
    workloadProperties.setProperty(OPERATION_COUNT_IDENTIFIER, String.valueOf(OPERATION_COUNT));
    workloadProperties.setProperty(READ_PROPORTION_IDENTIFIER, String.valueOf(READ_PROPORTION));
    workloadProperties.setProperty(UPDATE_PROPORTION_IDENTIFIER, String.valueOf(UPDATE_PROPORTION));
    workloadProperties.setProperty(SCAN_PROPORTION_IDENTIFIER, String.valueOf(SCAN_PROPORTION));
    workloadProperties.setProperty(INSERT_PROPORTION_IDENTIFIER, String.valueOf(INSERT_PROPORTION));
    workloadProperties.setProperty(REQUEST_DISTRIBUTION_IDENTIFIER, REQUEST_DISTRIBUTION);
  }

  @AfterClass
  public static void cleanup() {
    File testDir = new File(OUTPUT_DIRECTORY);
    if (testDir.exists() && testDir.isDirectory()) {
      for (File file : testDir.listFiles()) {
        file.delete();
      }
      testDir.delete();
    }
  }

  @Before
  public void init() {
    fileStoreClient = new FileStoreClient();
  }

  @Test
  public void saveToStandardDirectory() {
    try {
      fileStoreClient.init();
    } catch (DBException e) {
      e.printStackTrace();
      System.exit(2);
    }

    doTransactions();
  }

  @Test
  public void savePrettyPrintedToOwnDirectory() {
    Properties properties = new Properties();
    properties.setProperty(OUTPUT_DIRECTORY_IDENTIFIER, OUTPUT_DIRECTORY);
    properties.setProperty(ENABLE_PRETTY_PRINTING_IDENTIFIER, ENABLE_PRETTY_PRINTING);

    try {
      fileStoreClient.init();
    } catch (DBException e) {
      e.printStackTrace();
      System.exit(3);
    }

    doTransactions();
  }

  private void doTransactions() {
    Vector<HashMap<String, ByteIterator>> vector = new Vector<>();
    Map<String, ByteIterator> values = new HashMap<>();
    Map<String, ByteIterator> result = new HashMap<>();
    Set<String> set = new TreeSet<>();
    StringByteIterator firstValue = new StringByteIterator(ITERATOR_VALUE);
    StringByteIterator secondValue = new StringByteIterator(OTHER_ITERATOR_VALUE);

    values.put(MAP_KEY, firstValue);
    set.add(MAP_KEY);

    fileStoreClient.insert(TABLE, KEY, values);

    values.put(OTHER_MAP_KEY, secondValue);

    fileStoreClient.update(TABLE, KEY, values);
    fileStoreClient.read(TABLE, KEY, set, result);
    fileStoreClient.scan(TABLE, KEY, 1, set, vector);
    fileStoreClient.delete(TABLE, KEY);

    Assert.assertEquals(vector.get(0).get(MAP_KEY).toString(), values.get(MAP_KEY).toString());
    Assert.assertEquals(result.get(MAP_KEY).toString(), values.get(MAP_KEY).toString());
  }
}
