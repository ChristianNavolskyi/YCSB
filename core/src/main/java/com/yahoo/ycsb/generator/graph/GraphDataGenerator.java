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
import com.yahoo.ycsb.ByteIteratorAdapter;
import com.yahoo.ycsb.generator.Generator;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract class to generate {@link Graph}s and return {@link Node}s and {@link Edge}s given their ids.
 */
public abstract class GraphDataGenerator extends Generator<Graph> {

  public static final String KEY_IDENTIFIER = "Key";

  private static final String loadEdgeFileName = Edge.EDGE_IDENTIFIER + "load.json";
  private static final String loadNodeFileName = Node.NODE_IDENTIFIER + "load.json";
  private static final String runEdgeFileName = Edge.EDGE_IDENTIFIER + "run.json";
  private static final String runNodeFileName = Node.NODE_IDENTIFIER + "run.json";

  private final Map<Long, Edge> edgeMap = new HashMap<>();
  private final Map<Long, Node> nodeMap = new HashMap<>();
  private final File edgeFile;
  private final File nodeFile;

  Gson gson;
  Type valueType;
  Graph lastValue = new Graph();

  /**
   * @param directory  in which the files for nodes and edges should be stored/restored from.
   * @param isRunPhase presets node and edge ids if is run phase.
   * @throws IOException if the directory of the files can't be created.
   */
  GraphDataGenerator(String directory, boolean isRunPhase) throws IOException {
    GsonBuilder gsonBuilder = new GsonBuilder().registerTypeAdapter(ByteIterator.class, new ByteIteratorAdapter());
    gson = gsonBuilder.create();

    valueType = new TypeToken<Map<String, ByteIterator>>() {
    }.getType();

    if (!directory.endsWith(File.separator)) {
      directory = directory + File.separator;
    }

    File directoryFile = new File(directory);

    if (isRunPhase) {
      nodeFile = new File(directory + runNodeFileName);
      edgeFile = new File(directory + runEdgeFileName);

      Node.presetId(getLastId(nodeFile));
      Edge.presetId(getLastId(edgeFile));
    } else {
      nodeFile = new File(directory + loadNodeFileName);
      edgeFile = new File(directory + loadEdgeFileName);
    }

    if (!necessaryFilesAvailable(directoryFile, nodeFile, edgeFile)) {
      throw new IOException(getExceptionMessage());
    }
  }

  public static boolean checkDataPresent(String outputDirectory) {
    return new File(outputDirectory + loadNodeFileName).exists()
        && new File(outputDirectory + loadEdgeFileName).exists();
  }

  static String getKeyString(String key) {
    return KEY_IDENTIFIER + "-" + key + "-";
  }

  @Override
  public final Graph lastValue() {
    return lastValue;
  }

  @Override
  public final Graph nextValue() {
    Graph graph = new Graph();
    try {
      graph = createNextValue();
    } catch (IOException e) {
      e.printStackTrace();
    }

    storeGraphComponents(graph);

    return graph;
  }

  public Node getNode(long key) {
    return nodeMap.get(key);
  }

  public Edge getEdge(long key) {
    return edgeMap.get(key);
  }

  File getEdgeFile() {
    return edgeFile;
  }

  File getNodeFile() {
    return nodeFile;
  }

  private int getLastId(File file) throws IOException {
    List<String> lines = Files.readAllLines(file.toPath(), Charset.forName(new FileReader(file).getEncoding()));
    String lastEntry = lines.get(lines.size() - 1);

    return Integer.parseInt(lastEntry.split("-")[1]);
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

  abstract String getExceptionMessage();

  abstract Graph createNextValue() throws IOException;

  abstract boolean necessaryFilesAvailable(File directoryFile, File nodeFile, File edgeFile);
}