package com.yahoo.ycsb.generator.graph;


import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class GraphDataRecreatorTest {

  private static GraphDataRecreator graphDataRecreator;
  private static String testResources = "src" + File.separator + "test" + File.separator + "resources";

  @BeforeClass
  public static void setUp() throws IOException {
    graphDataRecreator = new GraphDataRecreator(testResources);
  }

  @Test
  public void test() {

  }

}