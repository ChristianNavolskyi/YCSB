package com.yahoo.ycsb.generator.graph;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestGraphDataRecreator {

  private static final int numberOfNodes = 100;
  private static GraphDataRecreator graphDataRecreator;
  private static String testResources = "src" + File.separator + "test" + File.separator + "resources";

  @Before
  public void initGraphDataRecreator() throws IOException {
    graphDataRecreator = new GraphDataRecreator(testResources, false);
  }

  @Test
  public void loadFilesTest() throws IOException {
    graphDataRecreator = new GraphDataRecreator(testResources, false);
  }

  @Test(expected = IOException.class)
  public void tryLoadingFilesButFailBecauseFolderDoesNotContainNecessaryFiles() throws IOException {
    graphDataRecreator = new GraphDataRecreator(System.getProperty("user.dir"), false);
  }

  @Test
  public void checkIfRecreationProducesCorrectGraphs() {
    List<Graph> graphList = new ArrayList<>();

    for (int i = 0; i < numberOfNodes; i++) {
      Graph graph = graphDataRecreator.createNextValue();
      graphList.add(graph);

      assertEquals(1, graph.getNodes().size());

      switch (graph.getNodes().get(0).getLabel()) {
      case "Product":
        assertEquals(2, graph.getEdges().size());
        break;
      case "Factory":
        assertEquals(0, graph.getEdges().size());
        break;
      default:
        assertEquals(1, graph.getEdges().size());
        break;
      }
    }

    assertEquals(numberOfNodes, graphList.size());

    // all contents in file read.
    assertEquals(0, graphDataRecreator.createNextValue().getNodes().size());
    assertEquals(0, graphDataRecreator.createNextValue().getEdges().size());
  }

  @Test
  public void checkIfRecreatedGraphsAreRetrievableByGet() {
    List<Graph> graphList = new ArrayList<>();

    for (int i = 0; i < numberOfNodes; i++) {
      graphList.add(graphDataRecreator.nextValue());
    }

    for (Graph graph : graphList) {
      for (Node node : graph.getNodes()) {
        assertEquals(node, graphDataRecreator.getNode(node.getId()));
      }

      for (Edge edge : graph.getEdges()) {
        assertEquals(edge, graphDataRecreator.getEdge(edge.getId()));
      }
    }
  }

  @Test
  public void lastValueTest() {
    Graph graph;

    for (int i = 0; i < numberOfNodes; i++) {
      graph = graphDataRecreator.createNextValue();
      Graph actual = graphDataRecreator.lastValue();
      assertEquals(graph, actual);
    }

    graphDataRecreator.createNextValue();
    graph = graphDataRecreator.lastValue();
    assertEquals(0, graph.getNodes().size());
    assertEquals(0, graph.getEdges().size());
  }

}