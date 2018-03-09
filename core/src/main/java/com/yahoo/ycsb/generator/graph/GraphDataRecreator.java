/*
  Copyright (c) 2018 YCSB contributors. All rights reserved.
  <p>
  Licensed under the Apache License, Version 2.0 (the "License"); you
  may not use this file except in compliance with the License. You
  may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  implied. See the License for the specific language governing
  permissions and limitations under the License. See accompanying
  LICENSE file.
 */

package com.yahoo.ycsb.generator.graph;

import com.google.gson.stream.JsonReader;
import com.yahoo.ycsb.ByteIterator;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class takes a data set of graph data and reproduces it.
 */
public class GraphDataRecreator extends GraphDataGenerator {

  private final List<Graph> graphs;
  private int currentPosition = -1;

  public GraphDataRecreator(String inputDirectory, boolean isRunPhase) throws IOException {
    super(inputDirectory, isRunPhase);

    Map<Long, Node> nodes = parseNodes(getNodeFile());
    Map<Long, Edge> edges = parseEdges(getEdgeFile(), nodes);
    graphs = createSingleGraphs(edges);
  }

  @Override
  String getExceptionMessage() {
    return "Graph data files aren't present.";
  }

  @Override
  Graph createNextValue() {
    if (hasValueAtNextPosition()) {
      lastValue = graphs.get(++currentPosition);
    } else {
      lastValue = new Graph();
    }

    return lastValue;
  }

  @Override
  boolean necessaryFilesAvailable(File directoryFile, File nodeFile, File edgeFile) {
    return directoryFile.exists() && directoryFile.isDirectory() && nodeFile.exists() && edgeFile.exists();
  }

  private List<JsonReader> getJsonReaders(File file) throws IOException {
    List<JsonReader> result = new ArrayList<>();
    List<String> components = getLinesOfStringsFromFile(file);

    for (int i = 0; i < components.size(); i++) {
      String component = components.get(i);
      String keyString = getKeyString(String.valueOf(i));

      if (component.startsWith(keyString)) {
        result.add(new JsonReader(new StringReader(component.substring(keyString.length()))));
      }
    }

    return result;
  }

  private List<String> getLinesOfStringsFromFile(File file) throws IOException {
    FileReader fileReader = new FileReader(file);
    return Files.readAllLines(file.toPath(), Charset.forName(fileReader.getEncoding()));
  }

  private Map<Long, Node> parseNodes(File nodeFile) {
    Map<Long, Node> result = new HashMap<>();

    try {
      List<JsonReader> jsonReaders = getJsonReaders(nodeFile);

      for (JsonReader jsonReader : jsonReaders) {
        Node node = getNodeFromJson(jsonReader);
        result.put(node.getId(), node);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return result;
  }

  private Node getNodeFromJson(JsonReader jsonReader) {
    Map<String, ByteIterator> values = gson.fromJson(jsonReader, valueType);

    return Node.recreateNode(values);
  }

  private Map<Long, Edge> parseEdges(File edgeFile, Map<Long, Node> nodeMap) {
    Map<Long, Edge> result = new HashMap<>();

    try {
      List<JsonReader> jsonReaders = getJsonReaders(edgeFile);

      for (JsonReader jsonReader : jsonReaders) {
        Edge edge = getEdgeFromJson(jsonReader, nodeMap);
        result.put(edge.getId(), edge);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return result;
  }

  private Edge getEdgeFromJson(JsonReader jsonReader, Map<Long, Node> nodeMap) {
    Map<String, ByteIterator> values = gson.fromJson(jsonReader, valueType);

    return Edge.recreateEdge(values, nodeMap);
  }

  private List<Graph> createSingleGraphs(Map<Long, Edge> edgeMap) {
    long lastNodeId = -1;
    List<Graph> result = new ArrayList<>();

    Node startNode;
    Node endNode;
    Graph graph;

    for (long i = 0; i < edgeMap.size(); i++) {
      Edge edge = edgeMap.get(i);
      graph = new Graph();

      startNode = edge.getStartNode();

      if (startNode.getId() > lastNodeId) {
        graph.addNode(startNode);
        lastNodeId = startNode.getId();
        result.add(graph);
        graph = new Graph();
      }

      endNode = edge.getEndNode();

      if (endNode.getId() > lastNodeId) {
        graph.addNode(endNode);
        graph.addEdge(edge);
        lastNodeId = endNode.getId();

        if (edgeMap.size() > i + 1 && edgeMap.get(i + 1).getEndNode().getId() <= lastNodeId) {
          Edge nextEdge = edgeMap.get(i + 1);
          graph.addEdge(nextEdge);
          i++;
        }

        result.add(graph);
      }
    }

    return result;
  }

  private boolean hasValueAtNextPosition() {
    return graphs != null && graphs.size() > currentPosition + 1;
  }
}
