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
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.GraphWorkload;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.yahoo.ycsb.workloads.GraphWorkload.*;

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

  public static final String NODE_IDENTIFIER = "Node";

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

  public static void presetId(int lastNodeId) {
    nodeIdCount = ++lastNodeId;
  }

  @Override
  public String getComponentTypeIdentifier() {
    return NODE_IDENTIFIER;
  }

  @Override
  public Map<String, ByteIterator> getHashMap() {
    java.util.HashMap<String, ByteIterator> values = new HashMap<>();

    values.put(GRAPH_ID_IDENTIFIER, new StringByteIterator(String.valueOf(this.getId())));
    values.put(GRAPH_LABEL_IDENTIFIER, new StringByteIterator(this.getLabel()));
    values.put(GRAPH_VALUE_IDENTIFIER, getStringByteIterator(GraphWorkload.getNodeByteSize()));
    return values;
  }

  private StringByteIterator getStringByteIterator(int nodeByteSize) {
    RandomByteIterator randomByteIterator = new RandomByteIterator(nodeByteSize);
    StringBuilder randomText = new StringBuilder();

    for (int i = 0; i < nodeByteSize; i++) {
      randomText.append(Character.toChars(randomByteIterator.nextByte()));
    }

    return new StringByteIterator(randomText.toString());
  }
}