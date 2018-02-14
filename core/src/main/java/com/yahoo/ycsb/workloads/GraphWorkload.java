package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.graph.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static com.yahoo.ycsb.generator.graph.Edge.EDGE_FIELDS_SET;
import static com.yahoo.ycsb.generator.graph.Node.NODE_FIELDS_SET;
import static com.yahoo.ycsb.workloads.CoreWorkload.*;
import static java.io.File.separatorChar;

/**
 * Workload class for graph databases.
 * <p>
 * Every node will have a size of 500 Bytes by default.
 * This can be changed via the {@value NODE_BYTE_SIZE_PROPERTY} parameter.
 * <p>
 * The recordCount property determines how many nodes will be inserted. The total amount of database inserts could
 * be higher due to edges being inserted.
 */
public class GraphWorkload extends Workload {

  public static final String GRAPH_ID_IDENTIFIER = "id";
  public static final String GRAPH_LABEL_IDENTIFIER = "label";
  public static final String GRAPH_VALUE_IDENTIFIER = "value";
  public static final String GRAPH_START_IDENTIFIER = "start";
  public static final String GRAPH_END_IDENTIFIER = "end";
  public static final String KEY_IDENTIFIER = "Key";

  //TODO Property to set only generating graph or running benchmark

  //TODO Two modes one for only generating data and one for loading that data iff present (else generates) and
  // running a benchmark

  //TODO Mechanism to retrieve data from files and insert them in the correct order (nodes and corresponding edges)

  /**
   * The name and default value of the property for the output directory for the files.
   */
  public static final String OUTPUT_DIRECTORY_PROPERTY = "outputDirectory";
  public static final String OUTPUT_DIRECTORY_DEFAULT = System.getProperty("user.dir")
      + separatorChar
      + "benchmarkingData"
      + separatorChar;

  private static final int RANDOM_NODE = 0;
  private static final int RANDOM_EDGE = 1;

  private static final String NODE_BYTE_SIZE_PROPERTY = "nodeByteSize";
  private static final String NODE_BYTE_SIZE_DEFAULT = "500";
  private static int nodeByteSize;

  private Random random = new Random();
  private GraphGenerator graphGenerator;
  private DiscreteGenerator discreteGenerator;
  private int maxScanLength;

  public static int getNodeByteSize() {
    return nodeByteSize;
  }

  public static int getKeyFromKeyString(String documentValue) {
    String result = documentValue.replaceAll("-", "");
    result = result.replaceAll(KEY_IDENTIFIER, "");
    result = result.split("\\{")[0];

    return Integer.valueOf(result);
  }

  @Override
  public void init(Properties properties) throws WorkloadException {
    super.init(properties);

    graphGenerator = new GraphGenerator(properties);
    discreteGenerator = CoreWorkload.createOperationGenerator(properties);

    nodeByteSize = Integer.parseInt(properties.getProperty(NODE_BYTE_SIZE_PROPERTY, NODE_BYTE_SIZE_DEFAULT));
    maxScanLength = Integer.parseInt(properties.getProperty(MAX_SCAN_LENGTH_PROPERTY,
        MAX_SCAN_LENGTH_PROPERTY_DEFAULT));

    String outputDirectory = getOutputDirectory(properties);

    try {
      graphGenerator.setStartIds(getLastIdOfType(outputDirectory, Node.NODE_IDENTIFIER),
          getLastIdOfType(outputDirectory, Edge.EDGE_IDENTIFIER));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String getOutputDirectory(Properties properties) throws WorkloadException {
    String outputDirectory = properties.getProperty(OUTPUT_DIRECTORY_PROPERTY, OUTPUT_DIRECTORY_DEFAULT);

    if (outputDirectory.charAt(outputDirectory.length() - 1) != separatorChar) {
      outputDirectory += separatorChar;
    }

    File directory = new File(outputDirectory);

    if (!directory.exists() && !directory.mkdirs()) {
      throw new WorkloadException("Could not read output directory for files with path: " + outputDirectory);
    }

    return outputDirectory;
  }

  private int getLastIdOfType(String outputDirectory, String typeIdentifier) throws IOException {
    String fileName = getDatabaseFileName(outputDirectory, typeIdentifier);
    List<String> strings = Files.readAllLines(Paths.get(fileName),
        Charset.forName(new FileReader(fileName).getEncoding()));
    return getKeyFromKeyString(strings.get(strings.size() - 1));
  }

  private String getDatabaseFileName(String outputDirectory, String table) {
    return outputDirectory + table + ".json";
  }

  @Override
  public boolean doInsert(DB db, Object threadState) {
    Graph graph = graphGenerator.nextValue();

    System.out.println("Inserting Nodes");
    for (Node node : graph.getNodes()) {
      if (!insertNode(db, node)) {
        return false;
      }
    }

    System.out.println("Inserting Edges");
    for (Edge edge : graph.getEdges()) {
      if (!insertEdge(db, edge)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean doTransaction(DB db, Object threadState) {
    String nextOperation = discreteGenerator.nextValue();

    if (nextOperation == null) {
      return false;
    }

    switch (nextOperation) {
    case READ_IDENTIFIER:
      System.out.println("Reading");
      doTransactionRead(db);
      break;
    case UPDATE_IDENTIFIER:
      System.out.println("Updating");
      doTransactionUpdate(db);
      break;
    case INSERT_IDENTIFIER:
      System.out.println("Inserting");
      doInsert(db, null);
      break;
    case SCAN_IDENTIFIER:
      System.out.println("Scanning");
      doTransactionScan(db);
      break;
    case READMODIFYWRITE_IDENTIFIER:
      System.out.println("Reading Modifying and Writing");
      doTransactionReadModifyWrite(db);
      break;
    default:
      return false;
    }
    return true;
  }

  private void doTransactionRead(DB db) {
    RandomGraphComponentChooser randomGraphComponentChooser = new RandomGraphComponentChooser();

    Map<String, ByteIterator> map = new HashMap<>();

    db.read(randomGraphComponentChooser.graphComponent.getComponentTypeIdentifier(),
        String.valueOf(randomGraphComponentChooser.graphComponent.getId()),
        randomGraphComponentChooser.fieldSet,
        map
    );

    printMap(map);
  }

  private void doTransactionUpdate(DB db) {
    RandomGraphComponentChooser randomGraphComponentChooser = new RandomGraphComponentChooser();

    Map<String, ByteIterator> map = new HashMap<>();

    db.read(randomGraphComponentChooser.graphComponent.getComponentTypeIdentifier(),
        String.valueOf(randomGraphComponentChooser.graphComponent.getId()),
        randomGraphComponentChooser.fieldSet,
        map);

    db.update(randomGraphComponentChooser.graphComponent.getComponentTypeIdentifier(),
        String.valueOf(randomGraphComponentChooser.graphComponent.getId()),
        map);

    printMap(map);
  }

  private void doTransactionScan(DB db) {
    RandomGraphComponentChooser randomGraphComponentChooser = new RandomGraphComponentChooser();

    Vector<HashMap<String, ByteIterator>> hashMaps = new Vector<>();

    db.scan(randomGraphComponentChooser.graphComponent.getComponentTypeIdentifier(),
        String.valueOf(randomGraphComponentChooser.graphComponent.getId()),
        maxScanLength,
        randomGraphComponentChooser.fieldSet,
        hashMaps
    );

    for (HashMap<String, ByteIterator> hashMap : hashMaps) {
      printMap(hashMap);
    }

  }

  private void doTransactionReadModifyWrite(DB db) {
    Node node = chooseRandomNode();
    Map<String, ByteIterator> values = new HashMap<>();

    db.read(node.getComponentTypeIdentifier(), String.valueOf(node.getId()), NODE_FIELDS_SET, values);

    System.out.println("old");
    printMap(values);

    values = node.getHashMap();

    System.out.println("new");
    printMap(values);

    db.update(node.getComponentTypeIdentifier(), String.valueOf(node.getId()), values);
  }

  private void printMap(Map<String, ByteIterator> map) {
    for (String s : map.keySet()) {
      System.out.println("Key: " + s + " Value: " + map.get(s));
    }
  }

  private Edge chooseRandomEdge() {
    int maxBound = (int) Edge.getEdgeIdCount() == 0 ? 1 : (int) Edge.getEdgeIdCount();

    int id = random.nextInt(maxBound);

    return Edge.recreateEdge(id);
  }

  private Node chooseRandomNode() {
    int maxBound = (int) Node.getNodeIdCount() == 0 ? 1 : (int) Node.getNodeIdCount();

    int id = random.nextInt(maxBound);

    return Node.recreateNode(id);
  }

  private int randomNodeOrEdge() {
    return random.nextInt(2);
  }

  private boolean insertNode(DB db, Node node) {
    Map<String, ByteIterator> values = node.getHashMap();

    System.out.println("Node: " + node.getId());
    printMap(values);

    return db.insert(node.getComponentTypeIdentifier(), String.valueOf(node.getId()), values).isOk();
  }

  private boolean insertEdge(DB db, Edge edge) {
    Map<String, ByteIterator> values = edge.getHashMap();

    System.out.println("Edge: " + edge.getId());
    printMap(values);

    return db.insert(edge.getComponentTypeIdentifier(), String.valueOf(edge.getId()), values).isOk();
  }

  private class RandomGraphComponentChooser {
    private GraphComponent graphComponent;
    private Set<String> fieldSet;

    RandomGraphComponentChooser() {
      this.fieldSet = new HashSet<>();

      choose();
    }

    private void choose() {
      switch (randomNodeOrEdge()) {
      case RANDOM_NODE:
        graphComponent = chooseRandomNode();
        fieldSet = NODE_FIELDS_SET;
        break;
      case RANDOM_EDGE:
        graphComponent = chooseRandomEdge();
        fieldSet = EDGE_FIELDS_SET;
        break;
      default:
        break;
      }
    }
  }
}
