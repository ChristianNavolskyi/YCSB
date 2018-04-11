/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.sparksee;

import com.sparsity.sparksee.gdb.LogLevel;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import static org.junit.Assert.assertEquals;

public class SparkseeClientTest {
  private static final String FIRST_NODE_KEY = "0";
  private static final String SECOND_NODE_KEY = "1";
  private static final String EDGE_KEY = "0";

  private static Map<String, ByteIterator> firstNodeValues;
  private static File basePath;
  private static Properties properties;
  private static Map<String, ByteIterator> secondNodeValues;
  private static Map<String, ByteIterator> edgeValues;
  private static int counter = 0;

  private SparkseeClient sparkseeClient;

  @BeforeClass
  public static void setUpClass() {
    properties = new Properties();

    firstNodeValues = new HashMap<>();
    firstNodeValues.put(Node.ID_IDENTIFIER, new StringByteIterator(FIRST_NODE_KEY));
    firstNodeValues.put(Node.LABEL_IDENTIFIER, new StringByteIterator("First"));

    secondNodeValues = new HashMap<>();
    secondNodeValues.put(Node.ID_IDENTIFIER, new StringByteIterator(SECOND_NODE_KEY));
    secondNodeValues.put(Node.LABEL_IDENTIFIER, new StringByteIterator("Second"));

    edgeValues = new HashMap<>();
    edgeValues.put(Edge.ID_IDENTIFIER, new StringByteIterator(EDGE_KEY));
    edgeValues.put(Edge.START_IDENTIFIER, new StringByteIterator(FIRST_NODE_KEY));
    edgeValues.put(Edge.END_IDENTIFIER, new StringByteIterator(SECOND_NODE_KEY));
    edgeValues.put(Edge.LABEL_IDENTIFIER, new StringByteIterator("connection"));
  }

  @Before
  public void setUpClient() throws DBException {
    basePath = new File("test" + counter++ + ".gdb");

    properties.setProperty(SparkseeClient.SPARKSEE_DATABASE_PATH_PROPERTY, basePath.getName());

    sparkseeClient = new SparkseeClient();
    sparkseeClient.setProperties(properties);
    sparkseeClient.init();
  }

  @Test
  public void testConfig() {
    System.out.println(LogLevel.valueOf("Fine").name());
  }

  @After
  public void tearDownClass() throws DBException {
    sparkseeClient.cleanup();
    basePath.delete();
  }

  @Test
  public void testInsert() {
    HashMap<String, ByteIterator> readValues = new HashMap<>();

    assertEquals(Status.OK, sparkseeClient.insert(Node.NODE_IDENTIFIER, FIRST_NODE_KEY, firstNodeValues));
    assertEquals(Status.OK, sparkseeClient.insert(Node.NODE_IDENTIFIER, SECOND_NODE_KEY, secondNodeValues));
    assertEquals(Status.OK, sparkseeClient.insert(Edge.EDGE_IDENTIFIER, EDGE_KEY, edgeValues));

    assertEquals(Status.OK, sparkseeClient.read(Node.NODE_IDENTIFIER, FIRST_NODE_KEY, firstNodeValues.keySet(), readValues));

    validateFields(firstNodeValues, readValues);
  }

  @Test
  public void testDelete() {
    HashMap<String, ByteIterator> readValues = new HashMap<>();

    assertEquals(Status.OK, sparkseeClient.insert(Node.NODE_IDENTIFIER, FIRST_NODE_KEY, firstNodeValues));
    assertEquals(Status.OK, sparkseeClient.insert(Node.NODE_IDENTIFIER, SECOND_NODE_KEY, secondNodeValues));

    assertEquals(Status.OK, sparkseeClient.delete(Node.NODE_IDENTIFIER, FIRST_NODE_KEY));

    assertEquals(Status.NOT_FOUND, sparkseeClient.read(Node.NODE_IDENTIFIER, FIRST_NODE_KEY, firstNodeValues.keySet(), new HashMap<>()));

    assertEquals(Status.OK, sparkseeClient.read(Node.NODE_IDENTIFIER, SECOND_NODE_KEY, secondNodeValues.keySet(), readValues));

    validateFields(secondNodeValues, readValues);
  }

  @Test
  public void testRead() {
    HashMap<String, ByteIterator> readValues = new HashMap<>();

    assertEquals(Status.OK, sparkseeClient.insert(Node.NODE_IDENTIFIER, FIRST_NODE_KEY, firstNodeValues));

    assertEquals(Status.OK, sparkseeClient.read(Node.NODE_IDENTIFIER, FIRST_NODE_KEY, firstNodeValues.keySet(), readValues));

    validateFields(firstNodeValues, readValues);
  }

  @Test
  public void testUpdate() {
    Map<String, ByteIterator> readValues = new HashMap<>();

    assertEquals(Status.OK, sparkseeClient.insert(Node.NODE_IDENTIFIER, FIRST_NODE_KEY, firstNodeValues));

    assertEquals(Status.OK, sparkseeClient.update(Node.NODE_IDENTIFIER, FIRST_NODE_KEY, secondNodeValues));

    assertEquals(Status.OK, sparkseeClient.read(Node.NODE_IDENTIFIER, FIRST_NODE_KEY, secondNodeValues.keySet(), readValues));

    validateFields(secondNodeValues, readValues);
  }

  @Test
  public void testScan() {
    int recordCount = 2;
    Vector<HashMap<String, ByteIterator>> scanVector = new Vector<>();

    assertEquals(Status.OK, sparkseeClient.insert(Node.NODE_IDENTIFIER, FIRST_NODE_KEY, firstNodeValues));
    assertEquals(Status.OK, sparkseeClient.insert(Node.NODE_IDENTIFIER, SECOND_NODE_KEY, secondNodeValues));
    assertEquals(Status.OK, sparkseeClient.insert(Edge.EDGE_IDENTIFIER, EDGE_KEY, edgeValues));

    assertEquals(Status.OK, sparkseeClient.scan(Node.NODE_IDENTIFIER,
        FIRST_NODE_KEY,
        recordCount,
        firstNodeValues.keySet(),
        scanVector));

    assertEquals(recordCount, scanVector.size());

    HashMap<String, ByteIterator> values = scanVector.get(0);
    assertEquals(firstNodeValues.keySet().size(), values.size());
    validateFields(firstNodeValues, values);

    values = scanVector.get(1);
    assertEquals(secondNodeValues.keySet().size(), values.size());
    validateFields(secondNodeValues, values);

    scanVector.clear();

    assertEquals(Status.OK, sparkseeClient.scan(Edge.EDGE_IDENTIFIER,
        EDGE_KEY,
        1,
        edgeValues.keySet(),
        scanVector));

    values = scanVector.get(0);
    assertEquals(edgeValues.keySet().size(), values.size());
    validateFields(edgeValues, values);
  }

  private void validateFields(Map<String, ByteIterator> originalValues, Map<String, ByteIterator> actualValues) {
    for (Map.Entry<String, ByteIterator> originalEntry : originalValues.entrySet()) {
      assertEquals(originalEntry.getValue().toString(), actualValues.get(originalEntry.getKey()).toString());
    }
  }
}