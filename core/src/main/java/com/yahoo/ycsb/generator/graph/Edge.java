package com.yahoo.ycsb.generator.graph;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Edge for the graph in the graph workload.
 */
public class Edge extends GraphComponent {
  public static final Set<String> EDGE_FIELDS_SET = new HashSet<>();
  private static final String START_IDENTIFIER = "start";
  private static final String END_IDENTIFIER = "end";
  private static long edgeIdCount = 0;

  static {
    EDGE_FIELDS_SET.add(ID_IDENTIFIER);
    EDGE_FIELDS_SET.add(LABEL_IDENTIFIER);
    EDGE_FIELDS_SET.add(START_IDENTIFIER);
    EDGE_FIELDS_SET.add(END_IDENTIFIER);
  }

  private Node startNode;
  private Node endNode;

  Edge(Node startNode, Node endNode, String label) {
    super(label, getAndIncrementIdCounter());

    this.startNode = startNode;
    this.endNode = endNode;
  }

  private synchronized static long getAndIncrementIdCounter() {
    return edgeIdCount++;
  }

  @Override
  public Map<String, ByteIterator> getHashMap() {
    HashMap<String, ByteIterator> values = new HashMap<>();

    values.put("label", new StringByteIterator(getLabel()));
    values.put("start", new StringByteIterator(String.valueOf(startNode.getId())));
    values.put("end", new StringByteIterator(String.valueOf(endNode.getId())));
    return values;
  }
}