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

  private final int testParameterCount;
  private final int productsPerOrder;

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
  private long count = 0;


  public GraphGenerator(Properties properties) {
    testParameterNodes = new ArrayList<>();

    Graph graph = new Graph();
    graph.addEdge(new Edge(new Node("start"), new Node("end"), "edge"));

    testParameterCount = Integer.valueOf(properties.getProperty(TEST_PARAMETER_COUNT_KEY,
        String.valueOf(TEST_PARAMETER_COUNT_DEFAULT_VALUE)));
    productsPerOrder = Integer.valueOf(properties.getProperty(PRODUCTS_PER_ORDER_KEY,
        String.valueOf(PRODUCTS_PER_ORDER_DEFAULT_VALUE)));

    lastValue = null;
  }

  @Override
  public Graph nextValue() {
    System.out.println("Generating value number: " + count);
    if (lastValue == null) {
      lastValue = buildInitialGraph();
    } else {
      lastValue = buildSubGraph();
    }

    count++;

    return lastValue;
  }

  @Override
  public Graph lastValue() {
    return lastValue;
  }

  private Graph buildInitialGraph() {
    Graph graph = new Graph();

    createInitialNodes();

    addInitialNodes(graph);
    addInitialEdges(graph);

    return graph;
  }

  private void addInitialNodes(Graph graph) {
    graph.addNode(this.factory);
    graph.addNode(this.orders);
    graph.addNode(this.machine);
    graph.addNode(this.design);
  }

  private void addInitialEdges(Graph graph) {
    graph.addEdge(new Edge(factory, machine, "owns"));
    graph.addEdge(new Edge(factory, orders, "receives"));
    graph.addEdge(new Edge(factory, design, "builds"));
  }

  private void createInitialNodes() {
    this.factory = new Node("Factory");
    this.orders = new Node("Orders");
    this.machine = new Node("Machine");
    this.design = new Node("Design");
  }

  private Graph buildSubGraph() {
    Graph subGraph = new Graph();

    if (orderCount == 0) {
      createNewOrder(subGraph);
      orderCount = productsPerOrder;
    } else {
      orderCount--;
    }

    createSubGraphNodes();

    addProductNodes(subGraph);
    addProductEdges(subGraph);

    return subGraph;
  }

  private void createNewOrder(Graph subGraph) {
    currentOrder = new Node("order");

    subGraph.addNode(currentOrder);
    subGraph.addEdge(new Edge(orders, currentOrder, "ordered"));
  }

  private void addProductNodes(Graph subGraph) {
    subGraph.addNode(product);
    subGraph.addNode(date);
    subGraph.addNode(tests);

    for (Node node : testParameterNodes) {
      subGraph.addNode(node);
    }
  }

  private void addProductEdges(Graph subGraph) {
    subGraph.addEdge(new Edge(product, date, "producedAt"));
    subGraph.addEdge(new Edge(product, tests, "tested"));
    subGraph.addEdge(new Edge(currentOrder, product, "ordered"));
    subGraph.addEdge(new Edge(machine, product, "produced"));

    for (Node testParameterNode : testParameterNodes) {
      subGraph.addEdge(new Edge(tests, testParameterNode, "hasTested"));
    }
  }

  private void createSubGraphNodes() {
    product = new Node("Product");
    date = new Node("Date");
    tests = new Node("Tests");

    for (int i = 0; i < testParameterCount; i++) {
      testParameterNodes.add(new Node("TestParameterNr:" + i));
    }
  }
}
