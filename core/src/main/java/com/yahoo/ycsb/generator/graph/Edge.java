package com.yahoo.ycsb.generator.graph;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.yahoo.ycsb.workloads.GraphWorkload.*;

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

  public static final String EDGE_IDENTIFIER = "Edge";
  private Node startNode;
  private Node endNode;

  Edge(Node startNode, Node endNode, String label) {
    super(label, getAndIncrementIdCounter());

    this.startNode = startNode;
    this.endNode = endNode;
  }

  private Edge(long id) {
    super(id);
  }

  public static Edge recreateEdge(long id) {
    return new Edge(id);
  }

  public static long getEdgeIdCount() {
    return edgeIdCount;
  }

  private static synchronized long getAndIncrementIdCounter() {
    return edgeIdCount++;
  }

  public static void presetId(int lastEdgeId) {
    edgeIdCount = ++lastEdgeId;
  }

  @Override
  public String getComponentTypeIdentifier() {
    return EDGE_IDENTIFIER;
  }

  @Override
  public Map<String, ByteIterator> getHashMap() {
    HashMap<String, ByteIterator> values = new HashMap<>();

    values.put(GRAPH_ID_IDENTIFIER, new StringByteIterator(String.valueOf(this.getId())));
    values.put(GRAPH_LABEL_IDENTIFIER, new StringByteIterator(this.getLabel()));
    values.put(GRAPH_START_IDENTIFIER, new StringByteIterator(String.valueOf(startNode.getId())));
    values.put(GRAPH_END_IDENTIFIER, new StringByteIterator(String.valueOf(endNode.getId())));
    return values;
  }
}