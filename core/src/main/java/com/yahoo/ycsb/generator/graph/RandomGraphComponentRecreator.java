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

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ListIterator;

/**
 * Class to recreate the saved value produced by a {@link RandomGraphComponentGenerator}, stored in a file in the
 * given directory.
 */
public class RandomGraphComponentRecreator extends RandomGraphComponentChooser {

  private ListIterator<String> nodeIterator;
  private ListIterator<String> edgeIterator;
  private ListIterator<String> componentIterator;


  public RandomGraphComponentRecreator(String inputDirectory,
                                       GraphDataGenerator graphDataGenerator) throws IOException {
    super(graphDataGenerator);

    nodeIterator = getIterator(inputDirectory + NODE_FILE_NAME);
    edgeIterator = getIterator(inputDirectory + EDGE_FILE_NAME);
    componentIterator = getIterator(inputDirectory + COMPONENT_FILE_NAME);
  }

  private ListIterator<String> getIterator(String edgeFile) throws IOException {
    return Files.readAllLines(Paths.get(edgeFile), Charset.forName(new FileReader(edgeFile)
        .getEncoding())).listIterator();
  }

  @Override
  long chooseRandomNodeId() {
    return getNextValue(nodeIterator);
  }

  @Override
  long chooseRandomEdgeId() {
    return getNextValue(edgeIterator);
  }

  @Override
  int randomNodeOrEdge() {
    return (int) getNextValue(componentIterator);
  }

  private long getNextValue(ListIterator<String> iterator) {
    if (iterator.hasNext()) {
      return Long.getLong(iterator.next());
    }

    return 0;
  }
}
