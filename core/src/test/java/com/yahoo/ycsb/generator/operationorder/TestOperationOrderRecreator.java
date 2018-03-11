package com.yahoo.ycsb.generator.operationorder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestOperationOrderRecreator {

  private static String inputDirectory = "src"
      + File.separator
      + "test"
      + File.separator
      + "resources";
  private static OperationOrderRecreator operationOrderRecreator;

  @Before
  public void setUp() throws IOException {
    operationOrderRecreator = new OperationOrderRecreator(inputDirectory);
  }

  @Test
  public void loadFile() throws IOException {
    operationOrderRecreator = new OperationOrderRecreator(inputDirectory);
  }

  @Test(expected = IOException.class)
  public void loadDirectoryWithoutNecessaryFile() throws IOException {
    operationOrderRecreator = new OperationOrderRecreator(System.getProperty("user.dir"));
  }

  @Test
  public void readLines() throws IOException {
    File file = new File(inputDirectory + "operations.txt");
    List<String> operations = Files.readAllLines(file.toPath(), Charset.forName(new FileReader(file).getEncoding()));

    for (String operation : operations) {
      String actual = operationOrderRecreator.nextValue();
      Assert.assertEquals(operation, actual);
    }
  }

  @Test
  public void testLastValue() {
    assertNull(operationOrderRecreator.lastValue());

    String operation = operationOrderRecreator.nextValue();

    assertEquals(operation, operationOrderRecreator.lastValue());

  }

}