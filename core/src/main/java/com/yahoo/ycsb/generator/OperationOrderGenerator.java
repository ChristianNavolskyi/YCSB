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

package com.yahoo.ycsb.generator;

import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This class generates {@link String}s which represent operations for a {@link com.yahoo.ycsb.DB}.
 * The value of that String is saves in a operations.txt file for later reproduction.
 */
public class OperationOrderGenerator extends Generator<String> implements Closeable {

  public static final String OPERATION_FILE_NAME = "operations.txt";
  private FileWriter fileWriter;
  private DiscreteGenerator discreteGenerator;
  private String lastOperation = "";

  /**
   * @param outputDirectory   for the operations.txt file to be stored.
   * @param discreteGenerator to generate the values to return and store.
   * @throws IOException if something is wrong with the output file/directory.
   */
  public OperationOrderGenerator(String outputDirectory, DiscreteGenerator discreteGenerator) throws IOException {
    fileWriter = new FileWriter(outputDirectory + OPERATION_FILE_NAME, true);
    this.discreteGenerator = discreteGenerator;
  }

  @Override
  public String nextValue() {
    lastOperation = discreteGenerator.nextValue();

    return lastValue();
  }

  @Override
  public String lastValue() {
    try {
      fileWriter.write(lastOperation);
      fileWriter.write("\n");
      fileWriter.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return lastOperation;
  }

  @Override
  public void close() throws IOException {
    fileWriter.close();
  }
}
