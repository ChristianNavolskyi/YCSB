package com.yahoo.ycsb.generator.operationorder;

import com.yahoo.ycsb.generator.DiscreteGenerator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This class generates {@link String}s which represent operations for a {@link com.yahoo.ycsb.DB}.
 * The value of that String is saves in a operations.txt file for later reproduction.
 */
public class OperationOrderRecorder extends OperationOrderGenerator {

  private FileWriter fileWriter;
  private DiscreteGenerator discreteGenerator;

  /**
   * @param outputDirectory   for the operations.txt file to be stored.
   * @param discreteGenerator to generate the values to return and store.
   * @throws IOException if something is wrong with the output file/directory.
   */
  OperationOrderRecorder(String outputDirectory, DiscreteGenerator discreteGenerator) throws IOException {
    super(outputDirectory);

    fileWriter = new FileWriter(getOperationFile(), true);
    this.discreteGenerator = discreteGenerator;
  }

  @Override
  public String nextValue() {
    lastOperation = discreteGenerator.nextValue();

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
  public String getExceptionMessage() {
    return "Could not create operation order file or it already present.";
  }

  @Override
  public boolean checkFiles(File directory, File... files) throws IOException {
    boolean directoryPresent = directory.exists() || directory.mkdirs();
    boolean filesCreated = true;

    for (File file : files) {
      filesCreated = filesCreated && file.createNewFile();
    }

    return directoryPresent && filesCreated;
  }
}
