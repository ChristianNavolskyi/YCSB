package com.yahoo.ycsb.generator.operationorder;

import com.yahoo.ycsb.generator.DiscreteGenerator;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestOperationOrderRecorder {

  private static String outputDirectory;
  private static OperationOrderRecorder operationOrderRecorder;
  private static DiscreteGenerator discreteGenerator;

  @BeforeClass
  public static void setUp() throws IOException {
    outputDirectory = System.getProperty("user.dir") + File.separator + "test";
    FileUtils.deleteDirectory(outputDirectory);

    discreteGenerator = new DiscreteGenerator();
    discreteGenerator.addValue(1 / 4.0, "read");
    discreteGenerator.addValue(1 / 4.0, "write");
    discreteGenerator.addValue(1 / 4.0, "scan");
    discreteGenerator.addValue(1 / 4.0, "update");
  }

  @Before
  public void initOperationRecorder() throws IOException {
    operationOrderRecorder = new OperationOrderRecorder(outputDirectory, discreteGenerator);
  }

  @After
  public void clearDirectory() throws IOException {
    FileUtils.deleteDirectory(outputDirectory);
  }

  @Test
  public void checkFilesCreated() {
    File file = new File(outputDirectory);

    assertEquals(1, Objects.requireNonNull(file.list()).length);
  }

  @Test(expected = IOException.class)
  public void checkFilesAlreadyPresent() throws IOException {
    operationOrderRecorder = new OperationOrderRecorder(outputDirectory, discreteGenerator);
  }

  @Test
  public void write100Operations() throws IOException {
    int numberOfOperations = 100;

    for (int i = 0; i < numberOfOperations; i++) {
      operationOrderRecorder.nextValue();
    }

    File file = new File(outputDirectory, "operations.txt");
    List<String> operations = Files.readAllLines(file.toPath(), Charset.forName(new FileReader(file).getEncoding()));

    assertEquals(numberOfOperations, operations.size());
  }

  @Test
  public void testLastValue() {
    assertNull(operationOrderRecorder.lastValue());

    String operation = operationOrderRecorder.nextValue();

    assertEquals(operation, operationOrderRecorder.lastValue());

  }
}