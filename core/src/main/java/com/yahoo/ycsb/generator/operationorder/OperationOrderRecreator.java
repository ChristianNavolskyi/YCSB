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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * This class takes a file with {@link com.yahoo.ycsb.DB} operations and returns one after another.
 */
public class OperationOrderRecreator extends OperationOrderGenerator {

  private Iterator<String> operations;

  /**
   * Takes the file with the operations and stores it for usage over {@code nextValue()} and {@code lastValue()}.
   *
   * @param outputDirectory where the file with the {@link com.yahoo.ycsb.DB} operations is located.
   */
  public OperationOrderRecreator(String outputDirectory) throws IOException {
    super(outputDirectory);

    String filename = outputDirectory + OPERATION_FILE_NAME;
    operations = Files.readAllLines(Paths.get(filename),
        Charset.forName(new FileReader(filename).getEncoding())).iterator();
  }

  @Override
  String getExceptionMessage() {
    return "Operation order file is not present.";
  }

  @Override
  boolean checkFiles(File directory, File operationsFile) {
    return directory.exists() && operationsFile.exists();
  }

  @Override
  public String nextValue() {
    if (operations.hasNext()) {
      lastOperation = operations.next();
      return lastOperation;
    } else {
      return "";
    }
  }
}
