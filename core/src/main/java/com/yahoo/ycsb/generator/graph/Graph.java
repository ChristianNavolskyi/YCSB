package com.yahoo.ycsb.generator.graph;

import java.util.List;

public class Graph {
  private List<Node> nodes;
  private List<Edge> edges;

  protected Graph() {
  }

  public List<Node> getNodes() {
    return nodes;
  }

  public List<Edge> getEdges() {
    return edges;
  }

  public void addNode(Node node) {
    this.nodes.add(node);
  }

  public void addEdge(Edge edge) {
    addNodeIfNotExisting(edge.getStartNode());
    addNodeIfNotExisting(edge.getEndNode());
    edges.add(edge);
  }

  private void addNodeIfNotExisting(Node node) {
    if (!nodes.contains(node.getId())) {
      addNode(node);
    }
  }

}