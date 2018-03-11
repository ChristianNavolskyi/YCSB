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

package com.yahoo.ycsb.generator.operationorder;

import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.StoringGenerator;

import java.io.File;
import java.io.IOException;

/**
 * This class generates {@link String}s which represent operations for a {@link com.yahoo.ycsb.DB}.
 * The value of that String is saves in a operations.txt file for later reproduction.
 */
public abstract class OperationOrderGenerator extends Generator<String> implements StoringGenerator {

  private static final String operationFileName = "operations.txt";
  private final File operationFile;

  String lastOperation;

  OperationOrderGenerator(String outputDirectory) throws IOException {
    File directory = new File(outputDirectory);
    operationFile = new File(outputDirectory, operationFileName);

    if (!checkFiles(directory, operationFile)) {
      throw new IOException(getExceptionMessage());
    }
  }

  public static OperationOrderGenerator create(String directory, DiscreteGenerator operationGenerator) throws IOException {
    if (checkDataPresent(directory)) {
      return new OperationOrderRecreator(directory);
    } else {
      return new OperationOrderRecorder(directory, operationGenerator);
    }
  }

  private static boolean checkDataPresent(String outputDirectory) {
    return new File(outputDirectory + operationFileName).exists();
  }

  @Override
  public final String lastValue() {
    return lastOperation;
  }

  final File getOperationFile() {
    return operationFile;
  }
}
