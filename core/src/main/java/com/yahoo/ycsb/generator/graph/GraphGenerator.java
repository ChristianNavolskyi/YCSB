/**
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.generator.graph;

import com.yahoo.ycsb.generator.Generator;

import java.util.Properties;

/**
 * Generates graph data with a industrial structure.
 * <p>
 * The factory has orders, machines and produces products by a design.
 * Every order has ten products which are all produced by the machine.
 * In the production process multiple tests are executed (128 by default) and the date of production is monitored.
 */
public class GraphGenerator extends Generator<Graph> {

  private static final String TEST_PARAMETER_COUNT_KEY = "testParameterCount";
  private static final int TEST_PARAMETER_COUNT_DEFAULT_VALUE = 128;
  private static final String PRODUCTS_PER_ORDER_KEY = "productsPerOrder";
  private static final int PRODUCTS_PER_ORDER_DEFAULT_VALUE = 10;

  private final int testParameterCount;
  private final int productsPerOrder;

  private int productPerOrderCounter = 0;
  private boolean shouldCreateProduct = true;
  private boolean shouldCreateDate = true;
  private boolean shouldCreateTests = true;
  private int testCounter = 0;

  private Graph lastValue;
  private Node factory;
  private Node orders;
  private Node machine;
  private Node design;
  private Node product;
  private Node date;
  private Node tests;
  private Node currentOrder;

  public GraphGenerator(Properties properties) {
    testParameterCount = Integer.valueOf(properties.getProperty(TEST_PARAMETER_COUNT_KEY,
        String.valueOf(TEST_PARAMETER_COUNT_DEFAULT_VALUE)));
    productsPerOrder = Integer.valueOf(properties.getProperty(PRODUCTS_PER_ORDER_KEY,
        String.valueOf(PRODUCTS_PER_ORDER_DEFAULT_VALUE)));
    lastValue = null;
  }

  public void setStartIds(int lastNodeId, int lastEdgeId) {
    Node.presetId(lastNodeId);
    Edge.presetId(lastEdgeId);
  }

  @Override
  public Graph nextValue() {
    lastValue = createGraphNode();
    return lastValue;
  }

  @Override
  public Graph lastValue() {
    if (lastValue != null) {
      return lastValue;
    } else {
      return nextValue();
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
      graph.addEdge(new Edge(factory, machine, "owns"));
    } else if (orders == null) {
      this.orders = new Node("Orders");
      graph.addNode(this.orders);
      graph.addEdge(new Edge(factory, orders, "receives"));
    } else if (design == null) {
      this.design = new Node("Design");
      graph.addNode(this.design);
      graph.addEdge(new Edge(factory, design, "builds"));
    } else if (productPerOrderCounter == 0) {
      currentOrder = new Node("order");
      graph.addNode(currentOrder);
      graph.addEdge(new Edge(orders, currentOrder, "ordered"));
      productPerOrderCounter = productsPerOrder;
    } else if (shouldCreateProduct) {
      product = new Node("Product");
      graph.addNode(product);
      graph.addEdge(new Edge(machine, product, "produced"));
      graph.addEdge(new Edge(currentOrder, product, "ordered"));
      shouldCreateProduct = false;
    } else if (shouldCreateDate) {
      date = new Node("Date");
      graph.addNode(date);
      graph.addEdge(new Edge(product, date, "producedAt"));
      shouldCreateDate = false;
    } else if (shouldCreateTests) {
      tests = new Node("Tests");
      graph.addNode(tests);
      graph.addEdge(new Edge(product, tests, "tested"));
      shouldCreateTests = false;
    } else if (testCounter < testParameterCount) {
      Node testParameterNode = new Node("TestParameterNr:" + testCounter);
      graph.addNode(testParameterNode);
      graph.addEdge(new Edge(tests, testParameterNode, "hasTested"));
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
}
