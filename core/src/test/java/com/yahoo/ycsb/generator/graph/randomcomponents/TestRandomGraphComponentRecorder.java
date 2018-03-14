package com.yahoo.ycsb.generator.graph.randomcomponents;

import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.GraphComponent;
import com.yahoo.ycsb.generator.graph.GraphDataGenerator;
import com.yahoo.ycsb.generator.graph.Node;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

public class TestRandomGraphComponentRecorder {

  private static String directory = System.getProperty("user.dir") + File.separator + "test";
  private static RandomGraphComponentRecorder randomGraphComponentRecorder;
  private static GraphDataGenerator generator;
  private int numberOfTimes = 100;

  @BeforeClass
  public static void initGenerator() throws IOException {
    generator = Mockito.mock(GraphDataGenerator.class);
    when(generator.getEdge(anyLong())).thenReturn(Edge.recreateEdge(1));
    when(generator.getNode(anyLong())).thenReturn(Node.recreateNode(1));
    FileUtils.deleteDirectory(directory);
  }

  @Before
  public void setUpRecorder() throws IOException {
    randomGraphComponentRecorder = new RandomGraphComponentRecorder(directory, generator);
  }

  @After
  public void clearDirectory() throws IOException {
    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void testCreationOfFiles() {
    File file = new File(directory);
    assertEquals(3, file.listFiles().length);
  }

  @Test(expected = IOException.class)
  public void createWithFilesPresent() throws IOException {
    new File(directory, "nodeIds.txt").createNewFile();

    new RandomGraphComponentRecorder(directory, generator);
  }

  @Test
  public void chooseRandomEdgeId() throws IOException {
    Edge.presetId(50);
    List<Long> results = new ArrayList<>();

    for (int i = 0; i < numberOfTimes; i++) {
      results.add(randomGraphComponentRecorder.chooseRandomEdgeId());
    }

    List<String> lines = getLines(new File(directory, "edgeIds.txt"));

    assertEquals(numberOfTimes, lines.size());

    for (int i = 0; i < results.size(); i++) {
      Long created = results.get(i);
      Long stored = Long.parseLong(lines.get(i));

      assertEquals(created, stored);
    }
  }

  @Test
  public void chooseRandomNodeId() throws IOException {
    Node.presetId(50);
    List<Long> results = new ArrayList<>();

    for (int i = 0; i < numberOfTimes; i++) {
      results.add(randomGraphComponentRecorder.chooseRandomNodeId());
    }

    List<String> lines = getLines(new File(directory, "nodeIds.txt"));

    assertEquals(numberOfTimes, lines.size());

    for (int i = 0; i < results.size(); i++) {
      Long created = results.get(i);
      Long stored = Long.parseLong(lines.get(i));

      assertEquals(created, stored);
    }
  }

  @Test
  public void chooseRandomNodeOrEdgeId() throws IOException {
    List<Integer> results = new ArrayList<>();

    for (int i = 0; i < numberOfTimes; i++) {
      results.add(randomGraphComponentRecorder.randomNodeOrEdge());
    }

    List<String> lines = getLines(new File(directory, "componentIds.txt"));

    assertEquals(numberOfTimes, lines.size());

    for (int i = 0; i < results.size(); i++) {
      Integer created = results.get(i);
      Integer stored = Integer.parseInt(lines.get(i));

      assertEquals(created, stored);
    }
  }

  @Test(timeout = 1000)
  public void testNextValue() {
    boolean hadNode = false;
    boolean hadEdge = false;

    while (!hadNode || !hadEdge) {
      GraphComponent randomComponent = randomGraphComponentRecorder.nextValue();

      assertNotNull(randomComponent);

      if (randomComponent.getComponentTypeIdentifier().equals(Edge.EDGE_IDENTIFIER)) {
        hadEdge = true;
      } else if (randomComponent.getComponentTypeIdentifier().equals(Node.NODE_IDENTIFIER)) {
        hadNode = true;
      }
    }
  }

  @Test
  public void testLastValue() {
    GraphComponent graphComponent = randomGraphComponentRecorder.nextValue();

    assertEquals(graphComponent, randomGraphComponentRecorder.lastValue());
  }

  private List<String> getLines(File file) throws IOException {
    return Files.readAllLines(file.toPath(), Charset.forName(new FileReader(file)
        .getEncoding()));
  }

}