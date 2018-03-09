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

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class takes a file with {@link com.yahoo.ycsb.DB} operations and returns one after another.
 */
public class OperationOrderRecreator extends Generator<String> {

  private int counter = 0;
  private List<String> operations;

  /**
   * Takes the file with the operations and stores it for usage over {@code nextValue()} and {@code lastValue()}.
   *
   * @param outputDirectory where the file with the {@link com.yahoo.ycsb.DB} operations is located.
   */
  public OperationOrderRecreator(String outputDirectory) {
    try {
      String filename = outputDirectory + OperationOrderGenerator.OPERATION_FILE_NAME;
      operations = Files.readAllLines(Paths.get(filename),
          Charset.forName(new FileReader(filename).getEncoding()));
    } catch (IOException e) {
      operations = new ArrayList<>();
      e.printStackTrace();
    }
  }

  @Override
  public String nextValue() {
    if (operations.size() < counter) {
      return operations.get(counter++);
    } else if (!operations.isEmpty()) {
      counter = 0;
      return nextValue();
    } else {
      return "";
    }
  }

  @Override
  public String lastValue() {
    if (counter > 0) {
      return operations.get(counter - 1);
    } else {
      return "";
    }
  }
}
