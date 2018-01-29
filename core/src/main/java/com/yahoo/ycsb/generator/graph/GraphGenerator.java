package com.yahoo.ycsb.generator.graph;

import com.yahoo.ycsb.generator.Generator;

import java.util.ArrayList;
import java.util.List;
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
  private static final String RECORDCOUNT_KEY = "recordcount";
  private static final int RECORDCOUNT_DEFAULT = 100;

  private final int testParameterCount;
  private final int productsPerOrder;
  private final int recordCount;

  private int orderCount = 0;
  private Graph lastValue;
  private Node factory;
  private Node orders;
  private Node machine;
  private Node design;
  private Node product;
  private Node date;
  private Node tests;
  private List<Node> testParameterNodes;
  private Node currentOrder;
  private long recordCounter = 0;

  public GraphGenerator(Properties properties) {
    testParameterNodes = new ArrayList<>();

    testParameterCount = Integer.valueOf(properties.getProperty(TEST_PARAMETER_COUNT_KEY,
        String.valueOf(TEST_PARAMETER_COUNT_DEFAULT_VALUE)));
    productsPerOrder = Integer.valueOf(properties.getProperty(PRODUCTS_PER_ORDER_KEY,
        String.valueOf(PRODUCTS_PER_ORDER_DEFAULT_VALUE)));
    recordCount = Integer.valueOf(properties.getProperty(RECORDCOUNT_KEY, String.valueOf(RECORDCOUNT_DEFAULT)));

    lastValue = null;
  }

  @Override
  public Graph nextValue() {
    if (recordCounter > recordCount) {
      return new Graph();
    } else if (lastValue == null) {
      lastValue = buildInitialGraph();
    } else {
      lastValue = buildSubGraph();
    }

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

  private Graph buildInitialGraph() {
    Graph graph = new Graph();
    if (recordCounter > recordCount) {
      return graph;
    }

    this.factory = new Node("Factory");
    addNode(graph, this.factory);
    if (recordCounter > recordCount) {
      return graph;
    }

    this.machine = new Node("Machine");
    addNode(graph, this.machine);
    graph.addEdge(new Edge(factory, machine, "owns"));
    if (recordCounter > recordCount) {
      return graph;
    }

    this.orders = new Node("Orders");
    addNode(graph, this.orders);
    graph.addEdge(new Edge(factory, orders, "receives"));
    if (recordCounter > recordCount) {
      return graph;
    }

    this.design = new Node("Design");
    addNode(graph, this.design);
    graph.addEdge(new Edge(factory, design, "builds"));

    return graph;
  }

  private Graph buildSubGraph() {
    Graph subGraph = new Graph();
    if (recordCounter > recordCount) {
      return subGraph;
    }

    if (orderCount == 0) {
      currentOrder = new Node("order");
      addNode(subGraph, currentOrder);
      subGraph.addEdge(new Edge(orders, currentOrder, "ordered"));
      orderCount = productsPerOrder;
      if (recordCounter > recordCount) {
        return subGraph;
      }
    } else {
      orderCount--;
    }

    product = new Node("Product");
    addNode(subGraph, product);
    subGraph.addEdge(new Edge(machine, product, "produced"));
    subGraph.addEdge(new Edge(currentOrder, product, "ordered"));
    if (recordCounter > recordCount) {
      return subGraph;
    }

    date = new Node("Date");
    addNode(subGraph, date);
    subGraph.addEdge(new Edge(product, date, "producedAt"));
    if (recordCounter > recordCount) {
      return subGraph;
    }

    tests = new Node("Tests");
    addNode(subGraph, tests);
    subGraph.addEdge(new Edge(product, tests, "tested"));
    if (recordCounter > recordCount) {
      return subGraph;
    }

    for (int i = 0; i < testParameterCount; i++) {
      testParameterNodes.add(new Node("TestParameterNr:" + i));
      addNode(subGraph, testParameterNodes.get(i));
      subGraph.addEdge(new Edge(tests, testParameterNodes.get(i), "hasTested"));
      if (recordCounter > recordCount) {
        return subGraph;
      }
    }

    return subGraph;
  }

  private void addNode(Graph graph, Node node) {
    graph.addNode(node);
    recordCounter++;
  }
}
