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

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Class to pick a random {@link GraphComponent} ({@link Node} or {@link Edge}).
 */
public class RandomGraphComponentGenerator extends RandomGraphComponentChooser {

  private FileWriter nodeFileWriter;
  private FileWriter edgeFileWriter;
  private FileWriter componentFileWriter;
  private Random random;

  public RandomGraphComponentGenerator(String outputDirectory,
                                       GraphDataGenerator graphDataGenerator) throws IOException {
    super(graphDataGenerator);

    nodeFileWriter = new FileWriter(outputDirectory + NODE_FILE_NAME);
    edgeFileWriter = new FileWriter(outputDirectory + EDGE_FILE_NAME);
    componentFileWriter = new FileWriter(outputDirectory + COMPONENT_FILE_NAME);

    this.random = new Random();
  }

  @Override
  long chooseRandomEdgeId() {
    int maxBound = (int) Edge.getEdgeIdCount() == 0 ? 1 : (int) Edge.getEdgeIdCount();

    int id = random.nextInt(maxBound);

    try {
      writeLine(edgeFileWriter, id);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return id;
  }

  @Override
  long chooseRandomNodeId() {
    int maxBound = (int) Node.getNodeIdCount() == 0 ? 1 : (int) Node.getNodeIdCount();

    int id = random.nextInt(maxBound);

    try {
      writeLine(nodeFileWriter, id);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return id;
  }

  @Override
  int randomNodeOrEdge() {
    int id = random.nextInt(2);

    try {
      writeLine(componentFileWriter, id);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return id;
  }

  private void writeLine(FileWriter fileWriter, int id) throws IOException {
    fileWriter.write(String.valueOf(id));
    fileWriter.write("\n");
    fileWriter.flush();
  }
}
