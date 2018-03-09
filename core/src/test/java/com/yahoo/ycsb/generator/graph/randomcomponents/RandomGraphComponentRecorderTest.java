package com.yahoo.ycsb.generator.graph.randomcomponents;

import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.GraphComponent;
import com.yahoo.ycsb.generator.graph.GraphDataGenerator;
import com.yahoo.ycsb.generator.graph.Node;
import com.yahoo.ycsb.generator.graph.randomcomponents.RandomGraphComponentRecorder;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

public class RandomGraphComponentRecorderTest {

  private static String directory = System.getProperty("user.dir") + File.separator + "test" + File.separator;
  private static RandomGraphComponentRecorder randomGraphComponentRecorder;
  private int numberOfTimes = 100;

  @BeforeClass
  public static void initGenerator() throws IOException {
    GraphDataGenerator generator = Mockito.mock(GraphDataGenerator.class);
    when(generator.getEdge(anyLong())).thenReturn(Edge.recreateEdge(1));
    when(generator.getNode(anyLong())).thenReturn(Node.recreateNode(1));
    FileUtils.deleteDirectory(directory);
    randomGraphComponentRecorder = new RandomGraphComponentRecorder(directory, generator);
  }

  @AfterClass
  public static void deleteDirectory() throws IOException {
    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void testCreationOfFiles() {
    File file = new File(directory);
    assertEquals(3, file.listFiles().length);
  }

  @Test
  public void chooseRandomEdgeId() throws IOException {
    Edge.presetId(50);
    List<Long> results = new ArrayList<>();

    for (int i = 0; i < numberOfTimes; i++) {
      results.add(randomGraphComponentRecorder.chooseRandomEdgeId());
    }

    String fileName = directory + "edgeIds.txt";
    List<String> lines = Files.readAllLines(Paths.get(fileName), Charset.forName(new FileReader(fileName)
        .getEncoding()));

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

    String fileName = directory + "nodeIds.txt";
    List<String> lines = Files.readAllLines(Paths.get(fileName), Charset.forName(new FileReader(fileName)
        .getEncoding()));

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

    String fileName = directory + "componentIds.txt";
    List<String> lines = Files.readAllLines(Paths.get(fileName), Charset.forName(new FileReader(fileName)
        .getEncoding()));

    assertEquals(numberOfTimes, lines.size());

    for (int i = 0; i < results.size(); i++) {
      Integer created = results.get(i);
      Integer stored = Integer.parseInt(lines.get(i));

      assertEquals(created, stored);
    }
  }

  @Test(timeout = 1000)
  public void testChoose() {
    boolean hadNode = false;
    boolean hadEdge = false;

    while (!hadNode || !hadEdge) {
      GraphComponent randomComponent = randomGraphComponentRecorder.choose();

      assertNotNull(randomComponent);

      if (randomComponent.getComponentTypeIdentifier().equals(Edge.EDGE_IDENTIFIER)) {
        hadEdge = true;
      } else if (randomComponent.getComponentTypeIdentifier().equals(Node.NODE_IDENTIFIER)) {
        hadNode = true;
      }
    }
  }

}