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

/**
 * Abstract class to pick a random {@link GraphComponent} ({@link Node} or {@link Edge}).
 */
public abstract class RandomGraphComponentChooser {

  static final String NODE_FILE_NAME = "nodeId.txt";
  static final String EDGE_FILE_NAME = "edgeId.txt";
  static final String COMPONENT_FILE_NAME = "randomComponent.txt";

  private static final int NODE = 0;
  private static final int EDGE = 1;

  private GraphDataGenerator graphDataGenerator;

  RandomGraphComponentChooser(GraphDataGenerator graphDataGenerator) {
    this.graphDataGenerator = graphDataGenerator;
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
