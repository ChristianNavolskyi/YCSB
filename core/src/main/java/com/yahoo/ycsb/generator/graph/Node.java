package com.yahoo.ycsb.generator.graph;

public class Node {
  private static long nodeIdCount = 0;

  private long id;

  private String label;

  protected Node(String label) {
    this.id = nodeIdCount++;
    this.label = label;
  }

  public long getId() {
    return id;
  }

  public String getLabel() {
    return label;
  }
}