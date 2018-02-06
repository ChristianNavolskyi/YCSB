package com.yahoo.ycsb.generator.graph;

import java.util.ArrayList;
import java.util.List;

/**
 * Graph for the graph workload.
 */
public class Graph {
  private List<Node> nodes;
  private List<Edge> edges;

  Graph() {
    nodes = new ArrayList<>();
    edges = new ArrayList<>();
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
    edges.add(edge);
  }
}