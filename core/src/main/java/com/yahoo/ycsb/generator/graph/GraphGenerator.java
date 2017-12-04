package com.yahoo.ycsb.generator.graph;

import com.yahoo.ycsb.generator.Generator;

import java.util.Map;
import java.util.Properties;

public class GraphGenerator extends Generator<Graph> {

  private Graph graph;


  public GraphGenerator(Properties properties) {
    graph = new Graph();
    graph.addEdge(new Edge(new Node("start"), new Node("end"), "edge"));
  }

  @Override
  public Graph nextValue() {
    return graph;
  }

  @Override
  public Graph lastValue() {
    return graph;
  }

}
