/*
  Copyright (c) 2018 YCSB contributors. All rights reserved.
  <p>
  Licensed under the Apache License, Version 2.0 (the "License"); you
  may not use this file except in compliance with the License. You
  may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  implied. See the License for the specific language governing
  permissions and limitations under the License. See accompanying
  LICENSE file.
 */

package com.yahoo.ycsb.generator.graph;

import com.yahoo.ycsb.ByteIterator;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Generates graph data with a industrial structure.
 * <p>
 * The factory has orders, machines and produces products by a design.
 * Every order has ten products which are all produced by the machine.
 * In the production process multiple tests are executed (128 by default) and the date of production is monitored.
 */
public class GraphDataRecorder extends GraphDataGenerator implements Closeable {

  public static final String KEY_IDENTIFIER = "Key";

  private static final String TEST_PARAMETER_COUNT_KEY = "testParameterCount";
  private static final int TEST_PARAMETER_COUNT_DEFAULT_VALUE = 128;
  private static final String PRODUCTS_PER_ORDER_KEY = "productsPerOrder";
  private static final int PRODUCTS_PER_ORDER_DEFAULT_VALUE = 10;

  private final int testParameterCount;
  private final int productsPerOrder;

  private Graph lastValue = null;
  private Node factory;
  private Node orders;
  private Node machine;
  private Node design;
  private Node product;
  private Node tests;
  private Node currentOrder;

  private int productPerOrderCounter = 0;
  private boolean shouldCreateProduct = true;
  private boolean shouldCreateDate = true;
  private boolean shouldCreateTests = true;
  private int testCounter = 0;

  private Map<String, FileWriter> fileWriterMap;

  public GraphDataRecorder(String outputDirectory, Properties properties) throws IOException {
    super(outputDirectory);

    testParameterCount = Integer.valueOf(properties.getProperty(TEST_PARAMETER_COUNT_KEY,
        String.valueOf(TEST_PARAMETER_COUNT_DEFAULT_VALUE)));
    productsPerOrder = Integer.valueOf(properties.getProperty(PRODUCTS_PER_ORDER_KEY,
        String.valueOf(PRODUCTS_PER_ORDER_DEFAULT_VALUE)));

    fileWriterMap = new HashMap<>();
  }

  static String getKeyString(String key) {
    return KEY_IDENTIFIER + "-" + key + "-";
  }

  @Override
  Graph createNextValue() {
    lastValue = createGraphNode();

    try {
      saveGraphContentsAndFillValueOfNodes(lastValue);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return lastValue;
  }

  @Override
  boolean necessaryFilesAvailable(File directoryFile, File nodeFile, File edgeFile) {
    try {
      return (directoryFile.exists() || directoryFile.mkdirs()) && nodeFile.createNewFile() && edgeFile
          .createNewFile();
    } catch (IOException cause) {
      throw new RuntimeException("Could not create files.", cause);
    }
  }

  @Override
  public Graph lastValue() {
    if (lastValue != null) {
      return lastValue;
    } else {
      return nextValue();
    }
  }

  @Override
  public void close() throws IOException {
    for (String key : fileWriterMap.keySet()) {
      fileWriterMap.get(key).close();
    }
  }

  private void saveGraphContentsAndFillValueOfNodes(Graph graph) throws IOException {
    for (Node node : graph.getNodes()) {
      insert(getNodeFile(), String.valueOf(node.getId()), node.getHashMap());
    }

    for (Edge edge : graph.getEdges()) {
      insert(getEdgeFile(), String.valueOf(edge.getId()), edge.getHashMap());
    }
  }

  private Graph createGraphNode() {
    Graph graph = new Graph();

    if (factory == null) {
      this.factory = new Node("Factory");
      graph.addNode(this.factory);
    } else if (machine == null) {
      this.machine = new Node("Machine");
      graph.addNode(this.machine);
      graph.addEdge(new Edge("owns", factory, machine));
    } else if (orders == null) {
      this.orders = new Node("Orders");
      graph.addNode(this.orders);
      graph.addEdge(new Edge("receives", factory, orders));
    } else if (design == null) {
      this.design = new Node("Design");
      graph.addNode(this.design);
      graph.addEdge(new Edge("builds", factory, design));
    } else if (productPerOrderCounter == 0) {
      currentOrder = new Node("order");
      graph.addNode(currentOrder);
      graph.addEdge(new Edge("ordered", orders, currentOrder));
      productPerOrderCounter = productsPerOrder;
    } else if (shouldCreateProduct) {
      product = new Node("Product");
      graph.addNode(product);
      graph.addEdge(new Edge("produced", machine, product));
      graph.addEdge(new Edge("ordered", currentOrder, product));
      shouldCreateProduct = false;
    } else if (shouldCreateDate) {
      Node date = new Node("Date");
      graph.addNode(date);
      graph.addEdge(new Edge("producedAt", product, date));
      shouldCreateDate = false;
    } else if (shouldCreateTests) {
      tests = new Node("Tests");
      graph.addNode(tests);
      graph.addEdge(new Edge("tested", product, tests));
      shouldCreateTests = false;
    } else if (testCounter < testParameterCount) {
      Node testParameterNode = new Node("TestParameterNr:" + testCounter);
      graph.addNode(testParameterNode);
      graph.addEdge(new Edge("hasTested", tests, testParameterNode));
      testCounter++;
    }

    if (testCounter >= testParameterCount - 1) {
      shouldCreateProduct = true;
      shouldCreateDate = true;
      shouldCreateTests = true;
      testCounter = 0;
      productPerOrderCounter--;
    }

    return graph;
  }

  private void insert(File file, String key, Map<String, ByteIterator> values) throws IOException {
    String output = gson.toJson(values, valueType);
    FileWriter outputStreamWriter = getOutputStreamWriter(file);

    writeToFile(key, output, outputStreamWriter);
  }

  private FileWriter getOutputStreamWriter(File file) throws IOException {
    if (!fileWriterMap.containsKey(file.getName())) {

      FileWriter outputStreamWriter = new FileWriter(file, true);
      fileWriterMap.put(file.getName(), outputStreamWriter);
    }

    return fileWriterMap.get(file.getName());
  }

  private void writeToFile(String key, String output, FileWriter outputStreamWriter) throws IOException {
    outputStreamWriter.write(getKeyString(key));
    outputStreamWriter.write(output);
    outputStreamWriter.write("\n");
    outputStreamWriter.flush();
  }
}
