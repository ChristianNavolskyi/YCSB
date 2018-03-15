package com.yahoo.ycsb.generator.graph;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGraphDataGenerator {

  private static File testDirectory;
  private static Properties properties;
  private static int numberOfNodes;

  @BeforeClass
  public static void setUp() {
    testDirectory = new File(System.getProperty("user.dir"), "test");

    properties = new Properties();
    properties.setProperty("testparametercount", "1");

    numberOfNodes = 1000;
  }

  @AfterClass
  public static void deleteDirectory() throws IOException {
    FileUtils.deleteDirectory(testDirectory);
  }

  @After
  public void clearDirectory() throws IOException {
    FileUtils.deleteDirectory(testDirectory);
  }

  @Test
  public void testLoadPhase() throws IOException {
    GraphDataGenerator recorder = GraphDataGenerator.create(testDirectory.getAbsolutePath(), false, properties);

    assertTrue(recorder instanceof GraphDataRecorder);

    ArrayList<Graph> graphs = new ArrayList<>();

    for (int i = 0; i < numberOfNodes; i++) {
      graphs.add(recorder.nextValue());
    }

    GraphDataGenerator recreator = GraphDataRecreator.create(testDirectory.getAbsolutePath(), false, properties);

    assertTrue(recreator instanceof GraphDataRecreator);

    compareRecreatedGraphs(graphs, recreator);
  }

  @Test
  public void testRunPhase() throws IOException {
    GraphDataGenerator recorder = GraphDataGenerator.create(testDirectory.getAbsolutePath(), true, properties);

    assertTrue(recorder instanceof GraphDataRecorder);

    ArrayList<Graph> graphs = new ArrayList<>();

    for (int i = 0; i < numberOfNodes; i++) {
      graphs.add(recorder.nextValue());
    }

    GraphDataGenerator recreator = GraphDataRecreator.create(testDirectory.getAbsolutePath(), true, properties);

    assertTrue(recreator instanceof GraphDataRecreator);

    compareRecreatedGraphs(graphs, recreator);
  }

  @Test
  public void testRunPhaseWithLoadData() throws IOException {
    GraphDataGenerator recorder = GraphDataGenerator.create(testDirectory.getAbsolutePath(), false, properties);

    assertTrue(recorder instanceof GraphDataRecorder);

    for (int i = 0; i < numberOfNodes; i++) {
      recorder.nextValue();
    }

    Node.presetId(-1);
    Edge.presetId(-1);

    recorder = GraphDataGenerator.create(testDirectory.getAbsolutePath(), true, properties);

    assertTrue(recorder instanceof GraphDataRecorder);

    ArrayList<Graph> graphs = new ArrayList<>();

    for (int i = 0; i < numberOfNodes; i++) {
      graphs.add(recorder.nextValue());
    }

    GraphDataGenerator recreator = GraphDataRecreator.create(testDirectory.getAbsolutePath(), true, properties);

    assertTrue(recreator instanceof GraphDataRecreator);

    compareRecreatedGraphs(graphs, recreator);
  }

  private void compareRecreatedGraphs(ArrayList<Graph> graphs, GraphDataGenerator recreator) {
    for (Graph originalGraph : graphs) {
      Graph recreatedGraph = recreator.nextValue();

      assertEquals(originalGraph.getNodes().size(), recreatedGraph.getNodes().size());
      assertEquals(originalGraph.getEdges().size(), recreatedGraph.getEdges().size());

      List<Node> originalNodes = originalGraph.getNodes();
      List<Node> recreatedNodes = recreatedGraph.getNodes();
      for (int i = 0; i < originalNodes.size(); i++) {
        Node originalNode = originalNodes.get(i);
        Node recreatedNode = recreatedNodes.get(i);

        assertEquals(originalNode.getId(), recreatedNode.getId());
        assertEquals(originalNode.getLabel(), recreatedNode.getLabel());
        assertEquals(originalNode.getHashMap().toString(), recreatedNode.getHashMap().toString());
      }

      List<Edge> originalEdges = originalGraph.getEdges();
      List<Edge> recreatedEdges = recreatedGraph.getEdges();
      for (int i = 0; i < originalEdges.size(); i++) {
        Edge originalEdge = originalEdges.get(i);
        Edge recreatedEdge = recreatedEdges.get(i);

        assertEquals(originalEdge.getId(), recreatedEdge.getId());
        assertEquals(originalEdge.getLabel(), recreatedEdge.getLabel());
        assertEquals(originalEdge.getStartNode().getId(), recreatedEdge.getStartNode().getId());
        assertEquals(originalEdge.getEndNode().getId(), recreatedEdge.getEndNode().getId());
      }
    }
  }
}
