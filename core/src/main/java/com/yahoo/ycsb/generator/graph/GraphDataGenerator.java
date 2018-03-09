/*
  Copyright (c) 2018 YCSB contributors. All rights reserved.
  <p>
  Licensed under the Apache License, Version 2.0 (the "License"); you
  may not use this edgeFile except in compliance with the License. You
  may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  implied. See the License for the specific language governing
  permissions and limitations under the License. See accompanying
  LICENSE edgeFile.
 */

package com.yahoo.ycsb.generator.graph;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.generator.Generator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract class to generate {@link Graph}s and return {@link Node}s and {@link Edge}s given their ids.
 */
public abstract class GraphDataGenerator extends Generator<Graph> {

  private final Map<Long, Edge> edgeMap = new HashMap<>();
  private final Map<Long, Node> nodeMap = new HashMap<>();
  private final File edgeFile;
  private final File nodeFile;
  Gson gson;
  Type valueType;

  /**
   * @param directory in which the files for nodes and edges should be stored/restored from.
   * @throws IOException if the directory of the files can't be created.
   */
  GraphDataGenerator(String directory) throws IOException {
    GsonBuilder gsonBuilder = new GsonBuilder().registerTypeAdapter(ByteIterator.class, new GraphDataRecreator.ByteIteratorAdapter());
    gson = gsonBuilder.create();

    valueType = new TypeToken<Map<String, ByteIterator>>() {
    }.getType();

    if (!directory.endsWith(File.separator)) {
      directory = directory + File.separator;
    }

    File directoryFile = new File(directory);
    edgeFile = new File(directory + "edges.json");
    nodeFile = new File(directory + "nodes.json");

    if (!necessaryFilesAvailable(directoryFile, nodeFile, edgeFile)) {
      throw new IOException("Files not available or could not be created.");
    }
  }

  File getEdgeFile() {
    return edgeFile;
  }

  File getNodeFile() {
    return nodeFile;
  }

  public final Node getNode(long key) {
    return nodeMap.get(key);
  }

  public final Edge getEdge(long key) {
    return edgeMap.get(key);
  }

  @Override
  public final Graph nextValue() {
    Graph graph = createNextValue();

    storeGraphComponents(graph);

    return graph;
  }

  private void storeGraphComponents(Graph graph) {
    for (Edge edge : graph.getEdges()) {
      if (!edgeMap.containsKey(edge.getId())) {
        edgeMap.put(edge.getId(), edge);
      }
    }

    for (Node node : graph.getNodes()) {
      if (!nodeMap.containsKey(node.getId())) {
        nodeMap.put(node.getId(), node);
      }
    }
  }

  abstract Graph createNextValue();

  abstract boolean necessaryFilesAvailable(File directoryFile, File nodeFile, File edgeFile);
}