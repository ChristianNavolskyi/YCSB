package com.yahoo.ycsb.generator.graph;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.GraphWorkload;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Nodes for the graph in the graph workload.
 */
public class Node extends GraphComponent {
  public static final Set<String> NODE_FIELDS_SET = new HashSet<>();
  private static final String VALUE_IDENTIFIER = "value";
  private static long nodeIdCount = 0;

  static {
    NODE_FIELDS_SET.add(ID_IDENTIFIER);
    NODE_FIELDS_SET.add(LABEL_IDENTIFIER);
    NODE_FIELDS_SET.add(VALUE_IDENTIFIER);
  }

  Node(String label) {
    super(label, getAndIncrementIdCounter());
  }

  private Node(long id) {
    super(id);
  }

  public static Node recreateNode(long id) {
    return new Node(id);
  }

  public static long getNodeIdCount() {
    return nodeIdCount;
  }

  private static synchronized long getAndIncrementIdCounter() {
    return nodeIdCount++;
  }

  @Override
  public Map<String, ByteIterator> getHashMap() {
    java.util.HashMap<String, ByteIterator> values = new HashMap<>();

    values.put("label", new StringByteIterator(getLabel()));
    values.put("value", new RandomByteIterator(GraphWorkload.getNodeByteSize()));
    return values;
  }
}