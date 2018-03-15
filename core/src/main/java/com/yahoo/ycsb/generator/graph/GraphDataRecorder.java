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

import com.google.gson.stream.JsonReader;
import com.yahoo.ycsb.ByteIterator;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
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

  private static final String TEST_PARAMETER_COUNT_KEY = "testparametercount";
  private static final int TEST_PARAMETER_COUNT_DEFAULT_VALUE = 128;
  private static final String PRODUCTS_PER_ORDER_KEY = "productsperorder";
  private static final int PRODUCTS_PER_ORDER_DEFAULT_VALUE = 10;

  private final int testParameterCount;
  private final int productsPerOrder;

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

  GraphDataRecorder(String outputDirectory, boolean isRunPhase, Properties properties) throws IOException {
    super(outputDirectory, isRunPhase);

    testParameterCount = Integer.valueOf(properties.getProperty(TEST_PARAMETER_COUNT_KEY,
        String.valueOf(TEST_PARAMETER_COUNT_DEFAULT_VALUE)));
    productsPerOrder = Integer.valueOf(properties.getProperty(PRODUCTS_PER_ORDER_KEY,
        String.valueOf(PRODUCTS_PER_ORDER_DEFAULT_VALUE)));

    fileWriterMap = new HashMap<>();

    if (isRunPhase) {
      File loadNodeFile = new File(outputDirectory, loadNodeFileName);
      File loadEdgeFile = new File(outputDirectory, loadEdgeFileName);

      if (checkDataPresentAndCleanIfSomeMissing(GraphDataRecorder.class.getSimpleName(), loadNodeFile, loadEdgeFile)) {
        int lastNodeId = getLastId(loadNodeFile);

        for (int i = 0; i <= lastNodeId; i++) {
          createGraph();
        }
      }
    }
  }

  private int getLastId(File file) throws IOException {
    List<String> lines = Files.readAllLines(file.toPath(), Charset.forName(new FileReader(file).getEncoding()));
    String lastEntry = lines.get(lines.size() - 1);

    Map<String, ByteIterator> values = gson.fromJson(new JsonReader(new StringReader(lastEntry)), valueType);

    return Integer.parseInt(values.get(Node.ID_IDENTIFIER).toString());
  }

  @Override
  public String getExceptionMessage() {
    return "Could not create graph data files or they are already present.";
  }

  @Override
  Graph createNextValue() throws IOException {
    lastValue = createGraph();

    saveGraphContentsAndFillValueOfNodes(lastValue);

    return lastValue;
  }

  @Override
  public boolean checkFiles(File directory, File... files) throws IOException {
    boolean directoryPresent = directory.exists() || directory.mkdirs();
    boolean filesCreated = true;

    for (File file : files) {
      filesCreated = filesCreated && file.createNewFile();
    }

    return directoryPresent && filesCreated;
  }

  @Override
  public void close() throws IOException {
    for (String key : fileWriterMap.keySet()) {
      fileWriterMap.get(key).close();
    }
  }

  private void saveGraphContentsAndFillValueOfNodes(Graph graph) throws IOException {
    for (Node node : graph.getNodes()) {
      insert(getNodeFile(), node.getHashMap());
    }

    for (Edge edge : graph.getEdges()) {
      insert(getEdgeFile(), edge.getHashMap());
    }
  }

  private Graph createGraph() {
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
      currentOrder = new Node("Order");
      graph.addNode(currentOrder);
      graph.addEdge(new Edge("ordered", orders, currentOrder));
      productPerOrderCounter = productsPerOrder;
    } else if (shouldCreateProduct) {
      product = new Node("Product");
      graph.addNode(product);
      graph.addEdge(new Edge("produced", machine, product));
      graph.addEdge(new Edge("includes", currentOrder, product));
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

    if (isTestingFinished()) {
      shouldCreateProduct = true;
      shouldCreateDate = true;
      shouldCreateTests = true;
      testCounter = 0;
      productPerOrderCounter--;
    }

    return graph;
  }

  private boolean isTestingFinished() {
    return testCounter == testParameterCount && !shouldCreateProduct && !shouldCreateDate && !shouldCreateTests;
  }

  private void insert(File file, Map<String, ByteIterator> values) throws IOException {
    String output = gson.toJson(values, valueType);
    FileWriter fileWriter = getFileWriter(file);

    writeToFile(output, fileWriter);
  }

  private FileWriter getFileWriter(File file) throws IOException {
    if (!fileWriterMap.containsKey(file.getName())) {

      FileWriter fileWriter = new FileWriter(file, true);
      fileWriterMap.put(file.getName(), fileWriter);
    }

    return fileWriterMap.get(file.getName());
  }

  private void writeToFile(String output, FileWriter fileWriter) throws IOException {
    fileWriter.write(output);
    fileWriter.write("\n");
    fileWriter.flush();
  }
}
