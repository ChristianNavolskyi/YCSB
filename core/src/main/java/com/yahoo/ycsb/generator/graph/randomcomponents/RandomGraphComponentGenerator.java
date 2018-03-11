/*
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

package com.yahoo.ycsb.generator.graph.randomcomponents;

import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.StoringGenerator;
import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.GraphComponent;
import com.yahoo.ycsb.generator.graph.GraphDataGenerator;
import com.yahoo.ycsb.generator.graph.Node;

import java.io.File;
import java.io.IOException;

/**
 * Abstract class to pick a random {@link GraphComponent} ({@link Node} or {@link Edge}).
 */
public abstract class RandomGraphComponentGenerator extends Generator<GraphComponent> implements StoringGenerator {

  private static final int NODE = 0;
  private static final int EDGE = 1;
  private static final String nodeFileName = "nodeIds.txt";
  private static final String edgeFileName = "edgeIds.txt";
  private static final String componentFileName = "componentIds.txt";

  private final File nodeFile;
  private final File edgeFile;
  private final File componentFile;
  private final GraphDataGenerator graphDataGenerator;
  private GraphComponent lastGraphComponent;

  RandomGraphComponentGenerator(String directory, GraphDataGenerator graphDataGenerator) throws IOException {
    this.graphDataGenerator = graphDataGenerator;

    File directoryFile = new File(directory);

    nodeFile = new File(directory, nodeFileName);
    edgeFile = new File(directory, edgeFileName);

    componentFile = new File(directory, componentFileName);

    if (!checkFiles(directoryFile, nodeFile, edgeFile, componentFile)) {
      throw new IOException(getExceptionMessage());
    }
  }

  //TODO move to factoryclass (or not?)
  public static RandomGraphComponentGenerator create(String directory, GraphDataGenerator graphDataGenerator) throws IOException {
    if (checkDataPresent(directory)) {
      return new RandomGraphComponentRecreator(directory, graphDataGenerator);
    } else {
      return new RandomGraphComponentRecorder(directory, graphDataGenerator);
    }
  }

  private static boolean checkDataPresent(String directory) {
    return new File(directory, nodeFileName).exists()
        && new File(directory, edgeFileName).exists()
        && new File(directory, componentFileName).exists();
  }

  @Override
  public final GraphComponent nextValue() {
    switch (randomNodeOrEdge()) {
    case NODE:
      lastGraphComponent = chooseRandomNode();
      break;
    case EDGE:
      lastGraphComponent = chooseRandomEdge();
      break;
    default:
      return null;
    }

    return lastGraphComponent;
  }

  @Override
  public final GraphComponent lastValue() {
    return lastGraphComponent;
  }

  public final Node chooseRandomNode() {
    long id = chooseRandomNodeId();

    return graphDataGenerator.getNode(id);
  }

  File getNodeFile() {
    return nodeFile;
  }

  File getEdgeFile() {
    return edgeFile;
  }

  File getComponentFile() {
    return componentFile;
  }

  private Edge chooseRandomEdge() {
    long id = chooseRandomEdgeId();

    return graphDataGenerator.getEdge(id);
  }

  abstract long chooseRandomNodeId();

  abstract long chooseRandomEdgeId();

  abstract int randomNodeOrEdge();
}