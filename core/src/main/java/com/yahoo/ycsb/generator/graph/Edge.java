/**
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

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
  public static final String EDGE_IDENTIFIER = "Edge";
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

  Edge(String label, Node startNode, Node endNode) {
    super(label, getAndIncrementIdCounter());

    this.startNode = startNode;
    this.endNode = endNode;
  }

  private Edge(long id, String label, Node startNode, Node endNode) {
    super(label, id);
    this.startNode = startNode;
    this.endNode = endNode;
  }

  private Edge(long id) {
    super(id);
  }

  public static Edge recreateEdge(long id) {
    return new Edge(id);
  }

  public static Edge recreateEdge(Map<String, ByteIterator> values) {
    long id = Long.getLong(values.get(ID_IDENTIFIER).toString());
    String label = values.get(LABEL_IDENTIFIER).toString();

    int startNodeId = Integer.getInteger(values.get(START_IDENTIFIER).toString());
    int endNodeId = Integer.getInteger(values.get(END_IDENTIFIER).toString());

    Node startNode = Node.recreateNode(startNodeId);
    Node endNode = Node.recreateNode(endNodeId);

    return new Edge(id, label, startNode, endNode);
  }

  public static Edge recreateEdge(Map<String, ByteIterator> values, Map<Long, Node> nodeMap) {
    long id = Long.valueOf(values.get(ID_IDENTIFIER).toString());
    String label = values.get(LABEL_IDENTIFIER).toString();
    long startNodeId = Integer.valueOf(values.get(START_IDENTIFIER).toString());
    long endNodeId = Integer.valueOf(values.get(END_IDENTIFIER).toString());

    Node startNode = nodeMap.get(startNodeId);
    Node endNode = nodeMap.get(endNodeId);

    if (startNode != null && endNode != null) {
      return new Edge(id, label, startNode, endNode);
    }

    return null;
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

  public Node getStartNode() {
    return startNode;
  }

  public Node getEndNode() {
    return endNode;
  }

  @Override
  public String getComponentTypeIdentifier() {
    return EDGE_IDENTIFIER;
  }

  @Override
  public Map<String, ByteIterator> getHashMap() {
    HashMap<String, ByteIterator> values = new HashMap<>();

    values.put(ID_IDENTIFIER, new StringByteIterator(String.valueOf(this.getId())));
    values.put(LABEL_IDENTIFIER, new StringByteIterator(this.getLabel()));
    values.put(START_IDENTIFIER, new StringByteIterator(String.valueOf(startNode.getId())));
    values.put(END_IDENTIFIER, new StringByteIterator(String.valueOf(endNode.getId())));
    return values;
  }

  @Override
  public Set<String> getFieldSet() {
    return EDGE_FIELDS_SET;
  }
}