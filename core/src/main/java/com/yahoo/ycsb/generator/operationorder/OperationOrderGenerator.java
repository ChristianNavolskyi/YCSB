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

package com.yahoo.ycsb.generator.operationorder;

import com.yahoo.ycsb.generator.Generator;

import java.io.File;
import java.io.IOException;

/**
 * This class generates {@link String}s which represent operations for a {@link com.yahoo.ycsb.DB}.
 * The value of that String is saves in a operations.txt file for later reproduction.
 */
public abstract class OperationOrderGenerator extends Generator<String> {
  static final String OPERATION_FILE_NAME = "operations.txt";
  String lastOperation;

  public OperationOrderGenerator(String outputDirectory) throws IOException {
    File directory = new File(outputDirectory);
    File operationsFile = new File(outputDirectory + OPERATION_FILE_NAME);

    if (!checkFiles(directory, operationsFile)) {
      throw new IOException(getExceptionMessage());
    }
  }

  @Override
  public final String lastValue() {
    return lastOperation;
  }

  abstract String getExceptionMessage();

  abstract boolean checkFiles(File directory, File operationsFile) throws IOException;
}
