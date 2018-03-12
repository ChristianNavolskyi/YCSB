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
  private static final String className = RandomGraphComponentGenerator.class.getSimpleName();

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

  public static RandomGraphComponentGenerator create(String directory, boolean isRunPhase, GraphDataGenerator graphDataGenerator) throws IOException {
    if (isRunPhase) {
      if (checkDataPresent(directory)) {
        System.out.println(className + " creating RECREATOR.");
        return new RandomGraphComponentRecreator(directory, graphDataGenerator);
      } else {
        System.out.println(className + " creating RECORDER.");
        return new RandomGraphComponentRecorder(directory, graphDataGenerator);
      }
    } else {
      System.out.println(className + " not needed during load phase. Nothing created.");
      return null;
    }
  }

  private static boolean checkDataPresent(String directory) {
    File nodeFile = new File(directory, nodeFileName);
    File edgeFile = new File(directory, edgeFileName);
    File componentFile = new File(directory, componentFileName);

    boolean nodeFileExists = nodeFile.exists();
    boolean edgeFileExists = edgeFile.exists();
    boolean componentFileExists = componentFile.exists();

    boolean allFilesPresent = nodeFileExists
        && edgeFileExists
        && componentFileExists;

    boolean allFilesAbsent = !nodeFileExists && !edgeFileExists && !componentFileExists;

    boolean someFilesAbsent = !allFilesPresent && !allFilesAbsent;

    if (someFilesAbsent) {
      if (!nodeFileExists) {
        System.out.println(className + " " + nodeFileName + " is missing.");
      }
      if (!edgeFileExists) {
        System.out.println(className + " " + edgeFileName + " is missing.");
      }
      if (!componentFileExists) {
        System.out.println(className + " " + componentFileName + " is missing.");
      }

      deleteAllFiles(nodeFile, edgeFile, componentFile);
    }

    return allFilesPresent;
  }

  private static void deleteAllFiles(File... files) {
    for (File file : files) {
      if (file.delete()) {
        System.out.println(className + " deleted " + file.getName() + ".");
      }
    }
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
