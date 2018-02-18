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
import java.util.List;

import static com.yahoo.ycsb.workloads.GraphWorkload.KEY_IDENTIFIER;

/**
 * This class takes a data set of graph data and reproduces it.
 */
public class GraphFileReader extends Generator<Graph> {

  List<Graph> graphs;
  int currentPosition = 0;

  public static JsonReader getJsonReader(String key, String filename) throws IOException {
    List<String> components = getLinesOfStringsFromFile(filename);
    String desiredComponent = "";
    String keyString = getKeyString(key);

    for (String component : components) {
      if (component.startsWith(keyString)) {
        desiredComponent = component.substring(keyString.length());
      }
    }

    return new JsonReader(new StringReader(desiredComponent));
  }

  public static List<String> getLinesOfStringsFromFile(String filename) throws IOException {
    FileReader fileReader = new FileReader(filename);
    return Files.readAllLines(Paths.get(filename), Charset.forName(fileReader.getEncoding()));
  }

  public static String getKeyString(String key) {
    return KEY_IDENTIFIER + "-" + key + "-";
  }

  public void init(String edgeFileContent, String nodeFileContent) {
    graphs = new ArrayList<>();

    List<Edge> edgeList = parseEdges(edgeFileContent);
    List<Node> nodeList = parseNodes(nodeFileContent);

    graphs = combineComponentsToGraphs(nodeList, edgeList);
  }

  private List<Node> parseNodes(String nodeFileContent) {


    return null;
  }

  //TODO parse file contents and insert in list
  private List<Edge> parseEdges(String edgeFileContent) {

    return null;
  }

  private List<Graph> combineComponentsToGraphs(List<Node> nodeList, List<Edge> edgeList) {
    return null;
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
