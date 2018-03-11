package com.yahoo.ycsb.generator.graph.randomcomponents;


import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.GraphDataGenerator;
import com.yahoo.ycsb.generator.graph.Node;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

public class TestRandomGraphComponentRecreator {

  private static RandomGraphComponentRecreator randomGraphComponentRecreator;
  private static String directory = "src" + File.separator + "test" + File.separator + "resources";
  private static GraphDataGenerator graphDataGenerator;

  @BeforeClass
  public static void setUp() throws IOException {
    graphDataGenerator = Mockito.mock(GraphDataGenerator.class);
    when(graphDataGenerator.getNode(anyLong())).thenReturn(Node.recreateNode(1));
    when(graphDataGenerator.getEdge(anyLong())).thenReturn(Edge.recreateEdge(1));

    randomGraphComponentRecreator = new RandomGraphComponentRecreator(directory, graphDataGenerator);
  }

  @Test
  public void testLoadingOfFiles() throws IOException {
    randomGraphComponentRecreator = new RandomGraphComponentRecreator(directory, graphDataGenerator);
  }

  @Test(expected = IOException.class)
  public void testLoadingOfFilesWithNoFilesPresent() throws IOException {
    randomGraphComponentRecreator = new RandomGraphComponentRecreator(System.getProperty("user.dir"),
        graphDataGenerator);
  }

  @Test
  public void chooseRandomEdgeId() throws IOException {
    List<String> edgeLines = Files.readAllLines(randomGraphComponentRecreator.getEdgeFile().toPath(),
        Charset.forName(new FileReader(randomGraphComponentRecreator.getEdgeFile()).getEncoding()));

    for (String edgeLine : edgeLines) {
      Assert.assertEquals(Long.parseLong(edgeLine), randomGraphComponentRecreator.chooseRandomEdgeId());
    }
  }

  @Test
  public void chooseRandomNodeId() throws IOException {
    List<String> nodeLines = Files.readAllLines(randomGraphComponentRecreator.getNodeFile().toPath(),
        Charset.forName(new FileReader(randomGraphComponentRecreator.getNodeFile()).getEncoding()));

    for (String nodeLine : nodeLines) {
      Assert.assertEquals(Long.parseLong(nodeLine), randomGraphComponentRecreator.chooseRandomNodeId());
    }
  }

  @Test
  public void chooseRandomNodeOrEdgeId() throws IOException {
    List<String> components = Files.readAllLines(randomGraphComponentRecreator.getComponentFile().toPath(),
        Charset.forName(new FileReader(randomGraphComponentRecreator.getComponentFile()).getEncoding()));

    for (String component : components) {
      Assert.assertEquals(Long.parseLong(component), randomGraphComponentRecreator.randomNodeOrEdge());
    }
  }
}