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

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.generator.Generator;

import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yahoo.ycsb.workloads.GraphWorkload.KEY_IDENTIFIER;

/**
 * This class takes a data set of graph data and reproduces it.
 */
public class GraphFileReader extends Generator<Graph> {

  private List<Graph> graphs;
  private int currentPosition = 0;

  private GsonBuilder gsonBuilder;
  private Gson gson;
  private Type valueType;

  public static JsonReader getJsonReader(String key, String filename) throws IOException {
    return getJsonReaders(filename).get(Integer.parseInt(key));
  }

  private static List<JsonReader> getJsonReaders(String filePath) throws IOException {
    List<JsonReader> result = new ArrayList<>();
    List<String> components = getLinesOfStringsFromFile(filePath);

    for (int i = 0; i < components.size(); i++) {
      String component = components.get(i);
      String keyString = getKeyString(String.valueOf(i));

      if (component.startsWith(keyString)) {
        result.add(new JsonReader(new StringReader(component.substring(keyString.length()))));
      }
    }

    return result;
  }

  public static List<String> getLinesOfStringsFromFile(String filename) throws IOException {
    FileReader fileReader = new FileReader(filename);
    return Files.readAllLines(Paths.get(filename), Charset.forName(fileReader.getEncoding()));
  }

  public static String getKeyString(String key) {
    return KEY_IDENTIFIER + "-" + key + "-";
  }

  public void init(String edgeFilePath, String nodeFilePath) {
    gsonBuilder = new GsonBuilder().registerTypeAdapter(ByteIterator.class, new ByteIteratorAdapter());
    gson = gsonBuilder.create();

    valueType = new TypeToken<Map<String, ByteIterator>>() {
    }.getType();

    graphs = new ArrayList<>();

    Map<Long, Node> nodeMap = parseNodes(nodeFilePath);
    List<Edge> edgeList = parseEdges(edgeFilePath, nodeMap);

    graphs = createSingleGraphs(edgeList);
  }

  private Map<Long, Node> parseNodes(String nodeFileContent) {
    Map<Long, Node> result = new HashMap<>();

    try {
      List<JsonReader> jsonReaders = getJsonReaders(nodeFileContent);

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

  private List<Edge> parseEdges(String edgeFileContent, Map<Long, Node> nodeMap) {
    List<Edge> result = new ArrayList<>();

    try {
      List<JsonReader> jsonReaders = getJsonReaders(edgeFileContent);

      for (JsonReader jsonReader : jsonReaders) {
        result.add(getEdgeFromJson(jsonReader, nodeMap));
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

  private List<Graph> createSingleGraphs(List<Edge> edgeList) {
    long lastNodeId = -1;
    List<Graph> result = new ArrayList<>();

    Node startNode = null;
    Node endNode = null;
    Graph graph;

    for (int i = 0; i < edgeList.size(); i++) {
      Edge edge = edgeList.get(i);
      graph = new Graph();

      if (startNode == null) {
        startNode = edge.getStartNode();
        endNode = edge.getEndNode();
      }

      if (startNode.getId() > lastNodeId) {
        graph.addNode(startNode);
        result.add(graph);
        lastNodeId = startNode.getId();
      }

      if (endNode.getId() > lastNodeId) {
        graph.addNode(endNode);
        graph.addEdge(edge);

        if (edgeList.size() > i + 1 && edgeList.get(i + 1).getId() <= lastNodeId) {
          Edge nextEdge = edgeList.get(i + 1);
          graph.addEdge(nextEdge);
          i++;
        }

        result.add(graph);
        lastNodeId = endNode.getId();
      }
    }

    return result;
  }

  @Override
  public Graph nextValue() {
    if (graphs != null && graphs.size() > currentPosition) {
      return graphs.get(currentPosition++);
    }
    return null;
  }

  @Override
  public Graph lastValue() {
    if (graphs != null && graphs.size() > currentPosition) {
      return graphs.get(currentPosition);
    }
    return null;
  }

  /**
   * Adapter class to convert ByteIterator into and back from Json.
   */
  public static class ByteIteratorAdapter implements JsonSerializer<ByteIterator>, JsonDeserializer<ByteIterator> {

    private final String typeIdentifier = "type";
    private final String propertyIdentifier = "properties";

    @Override
    public JsonElement serialize(ByteIterator byteIterator,
                                 Type type,
                                 JsonSerializationContext jsonSerializationContext) {
      JsonObject result = new JsonObject();
      result.add(typeIdentifier, new JsonPrimitive(byteIterator.getClass().getName()));
      result.add(propertyIdentifier, jsonSerializationContext.serialize(byteIterator));

      return result;
    }

    @Override
    public ByteIterator deserialize(JsonElement jsonElement,
                                    Type type,
                                    JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
      JsonObject jsonObject = jsonElement.getAsJsonObject();
      String typeString = jsonObject.get(typeIdentifier).getAsString();
      JsonElement element = jsonObject.get(propertyIdentifier);

      try {
        return jsonDeserializationContext.deserialize(element, Class.forName(typeString));
      } catch (ClassNotFoundException e) {
        throw new JsonParseException("Could not find class " + typeString, e);
      }
    }
  }
}
