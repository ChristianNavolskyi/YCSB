package com.yahoo.ycsb.generator.graph;

public class Edge {
  private static long edgeIdCount = 0;
  private long id;
  private Node startNode;
  private Node endNode;

  private String label;

  protected Edge(Node startNode, Node endNode, String label) {
    this.id = edgeIdCount++;
    this.startNode = startNode;
    this.endNode = endNode;
    this.label = label;
  }

  public long getId() {
    return id;
  }

  public Node getStartNode() {
    return startNode;
  }

  public Node getEndNode() {
    return endNode;
  }

  public String getLabel() {
    return label;
  }
}