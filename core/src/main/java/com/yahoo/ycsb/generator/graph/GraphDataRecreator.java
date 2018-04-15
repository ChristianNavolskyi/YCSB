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
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class takes a data set of graph data and reproduces it.
 */
public class GraphDataRecreator extends GraphDataGenerator {

  private final List<Graph> graphs;
  private Map<Long, Edge> loadEdgeMap = new HashMap<>();
  private int currentPosition = -1;
  private Map<Long, Node> loadNodeMap;

  GraphDataRecreator(String inputDirectory, boolean isRunPhase, Properties properties) throws IOException {
    super(inputDirectory, isRunPhase, properties);

    Map<Long, Node> nodes = parseNodes(getNodeFile());

    loadNodeMap = new HashMap<>();
    if (isRunPhase) {
      loadNodeMap = parseNodes(new File(inputDirectory, LOAD_NODE_FILE_NAME));
      addNodesFromLoadPhaseIfPresent(nodes, loadNodeMap);

      loadEdgeMap = parseEdges(new File(inputDirectory, LOAD_EDGE_FILE_NAME), nodes);
    }

    Map<Long, Edge> edges = parseEdges(getEdgeFile(), nodes);
    final Long lastLoadId = getLastLoadId(loadNodeMap);

    if (isOnlyCreateNodes()) {
      graphs = nodes.values().stream()
          .sorted(Comparator.comparing(GraphComponent::getId))
          .filter(node -> node.getId() > lastLoadId)
          .map(node -> {
              Graph graph = new Graph();
              graph.addNode(node);
              return graph;
            })
          .collect(Collectors.toList());
    } else {
      graphs = createSingleGraphs(edges, lastLoadId);
    }
  }

  @Override
  List<Graph> getGraphs(int numberOfGraphs) {
    List<Graph> singleGraphs;

    if (isOnlyCreateNodes()) {
      singleGraphs = loadNodeMap.values().stream()
          .sorted(Comparator.comparing(GraphComponent::getId))
          .map(node -> {
              Graph graph = new Graph();
              graph.addNode(node);
              return graph;
            })
          .collect(Collectors.toList());
    } else {
      singleGraphs = createSingleGraphs(loadEdgeMap, -1);
    }

    loadEdgeMap.clear();

    return singleGraphs;
  }

  @Override
  Graph createNextValue() {
    if (hasValueAtNextPosition()) {
      Graph graph = graphs.get(++currentPosition);
      setLastValue(graph);
    } else {
      setLastValue(new Graph());
    }

    return getLastValue();
  }

  @Override
  public String getExceptionMessage() {
    return "Graph data files aren't present.";
  }

  @Override
  public boolean checkFiles(File directory, File... files) {
    boolean directoryPresent = directory.exists() && directory.isDirectory();
    boolean filesCreated = true;

    for (File file : files) {
      filesCreated = filesCreated && file.exists();
    }

    return directoryPresent && filesCreated;
  }

  private List<JsonReader> getJsonReaders(File file) throws IOException {
    List<JsonReader> result = new ArrayList<>();
    List<String> components = getLinesOfStringsFromFile(file);

    for (String component : components) {
      result.add(new JsonReader(new StringReader(component)));
    }

    return result;
  }

  private List<String> getLinesOfStringsFromFile(File file) throws IOException {
    if (file.exists()) {
      FileReader fileReader = new FileReader(file);
      return Files.readAllLines(file.toPath(), Charset.forName(fileReader.getEncoding()));
    } else {
      return new ArrayList<>();
    }
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

  private Node getNodeFromJson(JsonReader jsonReader) {
    Map<String, ByteIterator> values = getGson().fromJson(jsonReader, getValueType());

    return Node.recreateNode(values);
  }

  private Edge getEdgeFromJson(JsonReader jsonReader, Map<Long, Node> nodeMap) {
    Map<String, ByteIterator> values = getGson().fromJson(jsonReader, getValueType());

    return Edge.recreateEdge(values, nodeMap);
  }

  // assert Edge::getStartNode::getId < Edge::getEndNode::getId. Only edges from smaller ids to larger ids.
  private List<Graph> createSingleGraphs(Map<Long, Edge> edgeMap, long lastNodeId) {
    List<Graph> result = new ArrayList<>();

    Node startNode;
    Node endNode;
    Graph graph = new Graph();

    for (Long id : new TreeMap<>(edgeMap).keySet()) {
      Edge edge = edgeMap.get(id);

      startNode = edge.getStartNode();
      endNode = edge.getEndNode();

      if (startNode.getId() > lastNodeId) {
        graph.addNode(startNode);
        lastNodeId = startNode.getId();
      }

      if (endNode.getId() > lastNodeId) {
        if (!graph.getNodes().isEmpty()) {
          result.add(graph);
          graph = new Graph();
        }

        graph.addNode(endNode);
        lastNodeId = endNode.getId();
      }

      graph.addEdge(edge);
    }

    if (!result.contains(graph)) {
      // add last graph if not already added
      result.add(graph);
    }

    return result;
  }

  private Long getLastLoadId(Map<Long, Node> nodeMap) {
    Set<Long> loadNodeIds = new TreeMap<>(nodeMap).keySet();

    Long[] loadIds = loadNodeIds.toArray(new Long[0]);

    if (loadIds.length > 0) {
      return loadIds[loadIds.length - 1];
    } else {
      return -1L;
    }
  }

  private boolean hasValueAtNextPosition() {
    return graphs != null && graphs.size() > currentPosition + 1;
  }

  private void addNodesFromLoadPhaseIfPresent(Map<Long, Node> nodes, Map<Long, Node> loadNodes) {
    if (!loadNodes.isEmpty()) {
      addNodesIfNotPresent(nodes, loadNodes);
    }
  }

  private void addNodesIfNotPresent(Map<Long, Node> nodes, Map<Long, Node> loadNodes) {
    for (Long id : loadNodes.keySet()) {
      if (!nodes.containsKey(id)) {
        nodes.put(id, loadNodes.get(id));
      }
    }
  }
}
