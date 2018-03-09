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

package com.yahoo.ycsb.generator.graph;

import java.io.File;
import java.io.IOException;

/**
 * Abstract class to pick a random {@link GraphComponent} ({@link Node} or {@link Edge}).
 */
public abstract class RandomGraphComponentChooser {

  private static final int NODE = 0;
  private static final int EDGE = 1;
  private final File nodeFile;
  private final File edgeFile;
  private final File componentFile;
  private GraphDataGenerator graphDataGenerator;

  RandomGraphComponentChooser(String directory, GraphDataGenerator graphDataGenerator) throws IOException {
    this.graphDataGenerator = graphDataGenerator;

    File directoryFile = new File(directory);
    nodeFile = new File(directory + "nodeIds.txt");
    edgeFile = new File(directory + "edgeIds.txt");
    componentFile = new File(directory + "componentIds.txt");

    checkFilesAvailable(directoryFile, nodeFile, edgeFile, componentFile);
  }

  protected abstract boolean checkFilesAvailable(File directoryFile, File nodeFile, File edgeFile, File componentFile) throws IOException;

  File getNodeFile() {
    return nodeFile;
  }

  File getEdgeFile() {
    return edgeFile;
  }

  File getComponentFile() {
    return componentFile;
  }

  public final GraphComponent choose() {
    switch (randomNodeOrEdge()) {
    case NODE:
      return chooseRandomNode();
    case EDGE:
      return chooseRandomEdge();
    default:
      return null;
    }
  }

  private Edge chooseRandomEdge() {
    long id = chooseRandomEdgeId();

    return graphDataGenerator.getEdge(id);
  }

  public final Node chooseRandomNode() {
    long id = chooseRandomNodeId();

    return graphDataGenerator.getNode(id);
  }

  abstract long chooseRandomNodeId();

  abstract long chooseRandomEdgeId();

  abstract int randomNodeOrEdge();
}
