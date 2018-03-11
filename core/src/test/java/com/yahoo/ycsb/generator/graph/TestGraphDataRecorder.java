package com.yahoo.ycsb.generator.graph;

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
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGraphDataRecorder {

  private static Properties properties = new Properties();
  private static String pathname = System.getProperty("user.dir") + File.separator + "test";
  private static File directory = new File(pathname);
  private final int graphsToCreate = 100;
  private GraphDataRecorder graphDataRecorder;

  @BeforeClass
  public static void initPropertiesAndClearDirectory() throws IOException {
    FileUtils.deleteDirectory(directory);
    properties.setProperty("testparametercount", "1");
  }

  @Before
  public void setUp() throws IOException {
    graphDataRecorder = new GraphDataRecorder(directory.getAbsolutePath(), false , properties);
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void creatingFilesTest() {
    Set<File> list = new HashSet<>(Arrays.asList(Objects.requireNonNull(directory.listFiles())));

    assertEquals(2, list.size());
    assertTrue(list.contains(graphDataRecorder.getNodeFile()));
    assertTrue(list.contains(graphDataRecorder.getEdgeFile()));
  }

  @Test(expected = IOException.class)
  public void shouldAbortBecauseFolderExists() throws IOException {
    directory.mkdirs();
    File nodeFile = new File(directory, "nodes.json");
    nodeFile.createNewFile();

    graphDataRecorder = new GraphDataRecorder(directory.getAbsolutePath(), false, properties);
  }

  @Test
  public void checkIfGraphComponentsAreStored() throws IOException {
    for (int i = 0; i < graphsToCreate; i++) {
      graphDataRecorder.createNextValue();
    }

    List<String> strings = Files.readAllLines(graphDataRecorder.getNodeFile().toPath(), Charset.forName(new FileReader(graphDataRecorder
        .getNodeFile()).getEncoding()));

    assertEquals(graphsToCreate, strings.size());
  }

  @Test
  public void checkIfGraphComponentsCanBeRetrievedByGet() {
    List<Graph> graphList = new ArrayList<>();

    for (int i = 0; i < graphsToCreate; i++) {
      graphList.add(graphDataRecorder.nextValue());
    }

    for (Graph graph : graphList) {
      for (Node node : graph.getNodes()) {
        assertEquals(node, graphDataRecorder.getNode(node.getId()));
      }
      for (Edge edge : graph.getEdges()) {
        assertEquals(edge, graphDataRecorder.getEdge(edge.getId()));
      }
    }
  }

  @Test
  public void testLastValue() throws IOException {
    Graph graph = graphDataRecorder.createNextValue();

    assertEquals(graph, graphDataRecorder.lastValue());
  }

  @Test
  public void testLastValueWithoutCallToNextValue() {
    assertEquals(0, graphDataRecorder.lastValue().getNodes().size());
    assertEquals(0, graphDataRecorder.lastValue().getEdges().size());
  }

  @Test(expected = IOException.class)
  public void testClose() throws IOException {
    graphDataRecorder.createNextValue();
    graphDataRecorder.close();
    graphDataRecorder.createNextValue();
  }
}