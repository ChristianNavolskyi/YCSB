package com.yahoo.ycsb.generator.graph;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraphDataGeneratorTest {

  private final int graphsToCreate = 10;
  private Properties properties = new Properties();
  private GraphDataRecorder graphDataRecorder;
  private GraphDataRecreator graphDataRecreator;
  private File directory;
  private String pathname;

  @Before
  public void setUp() {
    pathname = System.getProperty("user.dir") + File.separator + "test";
    directory = new File(pathname);
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void testRecorderCreatingFilesAndRecreatorReadingFiles() throws IOException {
    graphDataRecorder = getGraphDataRecorder();

    Set<File> list = new HashSet<>(Arrays.asList(Objects.requireNonNull(directory.listFiles())));

    assertEquals(2, list.size());
    assertTrue(list.contains(graphDataRecorder.getNodeFile()));
    assertTrue(list.contains(graphDataRecorder.getEdgeFile()));

    graphDataRecreator = getGraphDataRecreator();
  }

  @Test(expected = IOException.class)
  public void shouldAbortBecauseFolderExists() throws IOException {
    directory.mkdirs();
    File nodeFile = new File(directory + File.separator + "nodes.json");
    nodeFile.createNewFile();

    graphDataRecorder = getGraphDataRecorder();
  }

  @Test
  public void checkIfGraphComponentsAreStored() throws IOException {
    List<Graph> graphList = new ArrayList<>();

    graphDataRecorder = getGraphDataRecorder();
    for (int i = 0; i < graphsToCreate; i++) {
      graphList.add(graphDataRecorder.createNextValue());
    }

    graphDataRecreator = getGraphDataRecreator();
    for (Graph graph : graphList) {
      Graph reproducedGraph = graphDataRecreator.createNextValue();

      graph.getNodes().containsAll(reproducedGraph.getNodes());
      graph.getEdges().containsAll(reproducedGraph.getEdges());

      reproducedGraph.getNodes().containsAll(graph.getNodes());
      reproducedGraph.getEdges().containsAll(graph.getNodes());
    }

  }

  @Test
  public void checkIfGraphComponentsCanBeRetrievedByGet() throws IOException {
    List<Graph> graphList = new ArrayList<>();

    graphDataRecorder = getGraphDataRecorder();
    for (int i = 0; i < graphsToCreate; i++) {
      graphList.add(graphDataRecorder.createNextValue());
    }

    checkValues(graphList, graphDataRecorder);
  }

  @Test
  public void checkIfRecreatedGraphComponentsCanBeRetrievedByGet() throws IOException {
    List<Graph> graphList = new ArrayList<>();

    graphDataRecorder = getGraphDataRecorder();
    for (int i = 0; i < graphsToCreate; i++) {
      graphList.add(graphDataRecorder.createNextValue());
    }

    graphDataRecreator = getGraphDataRecreator();
    for (int i = 0; i < graphsToCreate; i++) {
      graphDataRecreator.createNextValue();
    }

    checkValues(graphList, graphDataRecreator);

  }

  private void checkValues(List<Graph> graphList, GraphDataGenerator graphDataGenerator) {
    for (Graph graph : graphList) {
      for (Node node : graph.getNodes()) {
        assertEquals(node, graphDataGenerator.getNode(node.getId()));
      }
      for (Edge edge : graph.getEdges()) {
        assertEquals(edge, graphDataGenerator.getEdge(edge.getId()));
      }
    }
  }

  private GraphDataRecorder getGraphDataRecorder() throws IOException {
    return new GraphDataRecorder(directory.getAbsolutePath(), properties);
  }

  private GraphDataRecreator getGraphDataRecreator() throws IOException {
    return new GraphDataRecreator(directory.getAbsolutePath());
  }
}