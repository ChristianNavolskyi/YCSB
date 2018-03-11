package com.yahoo.ycsb.generator.graph;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;

import static com.yahoo.ycsb.generator.graph.GraphDataGenerator.getKeyString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGraphDataRecorder {

  private static Properties properties = new Properties();
  private static String directoryName = System.getProperty("user.dir") + File.separator + "test";
  private static File directory = new File(directoryName);
  private final int graphsToCreate = 100;

  @BeforeClass
  public static void initPropertiesAndClearDirectory() throws IOException {
    FileUtils.deleteDirectory(directory);
    properties.setProperty("testparametercount", "1");
  }

  @Before
  public void setUp() {
    directory.mkdirs();
  }

  @After
  public void tearDown() throws IOException {
    Node.presetId(-1);
    Edge.presetId(-1);
    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void creatingFilesTestInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    Set<File> list = new HashSet<>(Arrays.asList(Objects.requireNonNull(directory.listFiles())));

    assertEquals(2, list.size());
    assertTrue(list.contains(graphDataRecorder.getNodeFile()));
    assertTrue(list.contains(graphDataRecorder.getEdgeFile()));
  }

  @Test
  public void creatingFilesTestInRunPhaseWithoutLoadFiles() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    Set<File> list = new HashSet<>(Arrays.asList(Objects.requireNonNull(directory.listFiles())));

    assertEquals(2, list.size());
    assertTrue(list.contains(graphDataRecorder.getNodeFile()));
    assertTrue(list.contains(graphDataRecorder.getEdgeFile()));
    assertEquals(0, Node.getNodeIdCount());
    assertEquals(0, Edge.getEdgeIdCount());
  }

  @Test
  public void creatingFilesTestInRunPhaseWithLoadFiles() throws IOException {
    File nodeFile = createComponentLoadFileFileInDirectory(Node.NODE_IDENTIFIER);
    File edgeFile = createComponentLoadFileFileInDirectory(Edge.EDGE_IDENTIFIER);

    nodeFile.createNewFile();
    edgeFile.createNewFile();

    int nodeId = 10;
    int edgeId = 12;

    setIdsToLoadFiles(nodeFile, edgeFile, nodeId, edgeId);

    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    Set<File> list = new HashSet<>(Arrays.asList(Objects.requireNonNull(directory.listFiles())));

    assertEquals(4, list.size());
    assertTrue(list.contains(graphDataRecorder.getNodeFile()));
    assertTrue(list.contains(graphDataRecorder.getEdgeFile()));
    assertEquals(++nodeId, Node.getNodeIdCount());
    assertEquals(++edgeId, Edge.getEdgeIdCount());
  }

  @Test(expected = IOException.class)
  public void shouldAbortInLoadPhaseBecauseFolderWithFilesExists() throws IOException {
    directory.mkdirs();
    File nodeFile = createComponentLoadFileFileInDirectory(Node.NODE_IDENTIFIER);
    nodeFile.createNewFile();

    getGraphDataRecorderInLoadPhase();
  }

  @Test(expected = IOException.class)
  public void shouldAbortInRunPhaseBecauseFolderWithFilesExists() throws IOException {
    directory.mkdirs();
    File nodeFile = createComponentRunFileFileInDirectory(Node.NODE_IDENTIFIER);
    nodeFile.createNewFile();

    getGraphDataRecorderInRunPhase();
  }

  @Test
  public void checkIfGraphComponentsAreStoredInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    for (int i = 0; i < graphsToCreate; i++) {
      graphDataRecorder.createNextValue();
    }

    List<String> strings = Files.readAllLines(graphDataRecorder.getNodeFile().toPath(),
        Charset.forName(new FileReader(graphDataRecorder.getNodeFile()).getEncoding()));

    assertEquals(graphsToCreate, strings.size());
  }

  @Test
  public void checkIfGraphComponentsAreStoredInRunPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    for (int i = 0; i < graphsToCreate; i++) {
      graphDataRecorder.createNextValue();
    }

    List<String> strings = Files.readAllLines(graphDataRecorder.getNodeFile().toPath(),
        Charset.forName(new FileReader(graphDataRecorder.getNodeFile()).getEncoding()));

    assertEquals(graphsToCreate, strings.size());
  }

  @Test
  public void checkIfGraphComponentsCanBeRetrievedByGetInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

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
  public void checkIfGraphComponentsCanBeRetrievedByGetInRunPhaseWithoutLoadFiles() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

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

    assertEquals(0, graphList.get(0).getNodes().get(0).getId());
    assertEquals(0, graphList.get(1).getEdges().get(0).getId());
  }

  @Test
  public void checkIfGraphComponentsCanBeRetrievedByGetInRunPhaseWithLoadFiles() throws IOException {
    int nodeId = 5;
    int edgeId = 16;

    File nodeFile = createComponentLoadFileFileInDirectory(Node.NODE_IDENTIFIER);
    File edgeFile = createComponentLoadFileFileInDirectory(Edge.EDGE_IDENTIFIER);

    nodeFile.createNewFile();
    edgeFile.createNewFile();

    setIdsToLoadFiles(nodeFile, edgeFile, nodeId, edgeId);

    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

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

    assertEquals(++nodeId, graphList.get(0).getNodes().get(0).getId());
    assertEquals(++edgeId, graphList.get(1).getEdges().get(0).getId());
  }

  @Test
  public void testLastValueInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    Graph graph = graphDataRecorder.createNextValue();

    assertEquals(graph, graphDataRecorder.lastValue());
  }

  @Test
  public void testLastValueInRunPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    Graph graph = graphDataRecorder.createNextValue();

    assertEquals(graph, graphDataRecorder.lastValue());
  }

  @Test
  public void testLastValueWithoutCallToNextValueInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    assertEquals(0, graphDataRecorder.lastValue().getNodes().size());
    assertEquals(0, graphDataRecorder.lastValue().getEdges().size());
  }

  @Test
  public void testLastValueWithoutCallToNextValueInRunPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    assertEquals(0, graphDataRecorder.lastValue().getNodes().size());
    assertEquals(0, graphDataRecorder.lastValue().getEdges().size());
  }

  @Test(expected = IOException.class)
  public void testCloseInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    graphDataRecorder.createNextValue();
    graphDataRecorder.close();
    graphDataRecorder.createNextValue();
  }

  @Test(expected = IOException.class)
  public void testCloseInRunPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    graphDataRecorder.createNextValue();
    graphDataRecorder.close();
    graphDataRecorder.createNextValue();
  }

  private GraphDataRecorder getGraphDataRecorderInLoadPhase() throws IOException {
    return new GraphDataRecorder(directoryName, false, properties);
  }

  private GraphDataRecorder getGraphDataRecorderInRunPhase() throws IOException {
    return new GraphDataRecorder(directoryName, true, properties);
  }

  private File createComponentLoadFileFileInDirectory(String componentIdentifier) {
    return new File(directory, componentIdentifier + "load.json");
  }

  private File createComponentRunFileFileInDirectory(String componentIdentifier) {
    return new File(directory, componentIdentifier + "run.json");
  }

  private void setIdsToLoadFiles(File nodeFile, File edgeFile, int nodeId, int edgeId) throws IOException {
    FileWriter fileWriter = new FileWriter(nodeFile);
    fileWriter.write(getKeyString(String.valueOf(nodeId)));
    fileWriter.close();

    fileWriter = new FileWriter(edgeFile);
    fileWriter.write(getKeyString(String.valueOf(edgeId)));
    fileWriter.close();
  }
}