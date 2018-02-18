/**
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.graph.*;
import org.apache.htrace.core.Tracer;

import java.io.*;
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

  //TODO Mechanism to retrieve data from files and insert them in the correct order (nodes and corresponding edges)

  /**
   * The name and default value of the property for the output directory for the files.
   */
  public static final String DATA_SET_DIRECTORY_PROPERTY = "dataSetDirectory";
  public static final String DATA_SET_DIRECTORY_DEFAULT = System.getProperty("user.dir")
      + separatorChar
      + "benchmarkingData"
      + separatorChar;
  public static final String BIN_BINDINGS_PROPERTIES = "/../bin/bindings.properties";
  private static final int RANDOM_NODE = 0;
  private static final int RANDOM_EDGE = 1;
  private static final String NODE_BYTE_SIZE_PROPERTY = "nodeByteSize";
  private static final String NODE_BYTE_SIZE_DEFAULT = "500";
  private static final String FILESTORE_NAME = "filestore";
  private static int nodeByteSize;

  private Random random = new Random();
  private GraphGenerator graphGenerator;
  private GraphFileReader graphFileReader;
  private DiscreteGenerator discreteGenerator;
  private int maxScanLength;
  private Mode mode;
  private DB fileStoreDb;

  // Modes:
  // 4 - doNothing                                  - db == filestore && dataSetPresent && (load || run)
  // 1 - generate only dataSet                      - db == filestore && !dataSetPresent && (load || run)
  // 2 - generate dataSet and run benchmark         - db != filestore && dataSetPresent && (load || run)
  // 3 - only run benchmark with supplied dataSet   - db != filestore && !dataSetPresent && (load || run)

  public static int getNodeByteSize() {
    return nodeByteSize;
  }

  public static int getKeyFromKeyString(String documentValue) {
    String result = documentValue.replaceAll("-", "");
    result = result.replaceAll(KEY_IDENTIFIER, "");
    result = result.split("\\{")[0];

    return Integer.valueOf(result);
  }

  //TODO Two modes one for only generating data and one for loading that data iff present (else generates) and
  // running a benchmark
  @Override
  public void init(Properties properties) throws WorkloadException {
    super.init(properties);

    graphGenerator = new GraphGenerator(properties);
    discreteGenerator = CoreWorkload.createOperationGenerator(properties);

    nodeByteSize = Integer.parseInt(properties.getProperty(NODE_BYTE_SIZE_PROPERTY, NODE_BYTE_SIZE_DEFAULT));
    maxScanLength = Integer.parseInt(properties.getProperty(MAX_SCAN_LENGTH_PROPERTY,
        MAX_SCAN_LENGTH_PROPERTY_DEFAULT));


    boolean shouldRunWorkload = shouldRunWorkloadOrJustGenerateDataSet(properties);

    String outputDirectory = getOutputDirectory(properties);
    boolean benchmarkingDataPresent = checkBenchmarkingDataPresent(outputDirectory);

    mode = Mode.getMode(shouldRunWorkload, benchmarkingDataPresent);

    if (mode == Mode.RUN_BENCHMARK) {
      try {
        graphGenerator.setStartIds(getLastIdOfType(outputDirectory, Node.NODE_IDENTIFIER),
            getLastIdOfType(outputDirectory, Edge.EDGE_IDENTIFIER));
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    if (mode == Mode.GENERATE_DATA_AND_RUN_BENCHMARK) {
      initiateFileStoreDb(properties);
    }
  }

  @Override
  public void cleanup() throws WorkloadException {
    if (fileStoreDb != null) {
      try {
        fileStoreDb.cleanup();
      } catch (DBException e) {
        e.printStackTrace();
      }
    }
    super.cleanup();
  }

  //TODO Load initial Data into DB
  @Override
  public boolean doInsert(DB db, Object threadState) {
    Graph graph;
    boolean success;
    switch (mode) {
    case DO_NOTHING:
      break;
    case RUN_BENCHMARK:
      fillDbAsNormalWithDataFromPresentDataSet();
      graph = graphFileReader.nextValue();
      break;
    case GENERATE_DATA_AND_RUN_BENCHMARK:
      graph = graphGenerator.nextValue();
      success = insertGraphComponents(fileStoreDb, graph);
      success = success && insertGraphComponents(db, graph);

      return success;
    case GENERATE_DATA:
      graph = graphGenerator.nextValue();
      success = insertGraphComponents(db, graph);

      return success;
    default:
      return false;
    }
    return true;
  }

  private void fillDbAsNormalWithDataFromPresentDataSet() {

  }

  private boolean insertGraphComponents(DB db, Graph graph) {
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

  // TODO Iff operations present run from these else create, save and run.
  @Override
  public boolean doTransaction(DB db, Object threadState) {
    switch (mode) {
    case GENERATE_DATA:
    case DO_NOTHING:
      break;
    case RUN_BENCHMARK:
    case GENERATE_DATA_AND_RUN_BENCHMARK:
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
        doTransactionInsert(db);
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
      break;
    default:
      return false;
    }
    return true;
  }

  //TODO save in OperationsFile iff needed
  private void doTransactionInsert(DB db) {
    Graph graph = graphGenerator.nextValue();

    System.out.println("Inserting Nodes");
    for (Node node : graph.getNodes()) {
      insertNode(db, node);
    }

    System.out.println("Inserting Edges");
    for (Edge edge : graph.getEdges()) {
      insertEdge(db, edge);
    }
  }

  //TODO save in OperationsFile iff needed
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

  //TODO save in OperationsFile iff needed
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

  //TODO save in OperationsFile iff needed
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

  //TODO save in OperationsFile iff needed
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

  private boolean shouldRunWorkloadOrJustGenerateDataSet(Properties properties) {
    String db = properties.getProperty(Client.DB_PROPERTY);

    return db != null && !isDbFileStoreClient(db);
  }

  private boolean checkBenchmarkingDataPresent(String outputDirectory) {
    String nodeFileName = getDatabaseFileName(outputDirectory, Node.NODE_IDENTIFIER);
    String edgeFileName = getDatabaseFileName(outputDirectory, Edge.EDGE_IDENTIFIER);

    File nodeFile = new File(nodeFileName);
    File edgeFile = new File(edgeFileName);

    if (nodeFile.exists() && edgeFile.exists()) {
      return true;
    } else {
      deleteExistingFile(nodeFile, edgeFile);
    }

    return false;
  }

  private void deleteExistingFile(File nodeFile, File edgeFile) {
    if (nodeFile.exists()) {
      nodeFile.delete();
    }
    if (edgeFile.exists()) {
      edgeFile.delete();
    }
  }

  private void initiateFileStoreDb(Properties properties) {
    try {
      fileStoreDb = DBFactory.newDB(getBindingsProperties().getProperty(FILESTORE_NAME), properties, Tracer
          .curThreadTracer());
    } catch (UnknownDBException e) {
      e.printStackTrace();
    }
  }

  private boolean isDbFileStoreClient(String db) {
    Properties properties = getBindingsProperties();

    return properties.keySet().contains(db) && db.equals(FILESTORE_NAME);
  }

  private Properties getBindingsProperties() {
    File propertiesFile = new File(System.getProperty("user.dir") + BIN_BINDINGS_PROPERTIES);

    Properties properties = new Properties();

    try {
      InputStream inputStream = new FileInputStream(propertiesFile);
      properties.load(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return properties;
  }

  private String getOutputDirectory(Properties properties) throws WorkloadException {
    String outputDirectory = properties.getProperty(DATA_SET_DIRECTORY_PROPERTY, DATA_SET_DIRECTORY_DEFAULT);

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

  private enum Mode {
    // TODO used everywhere correctly?
    RUN_BENCHMARK(true, true),
    GENERATE_DATA_AND_RUN_BENCHMARK(true, false),
    DO_NOTHING(false, true),
    GENERATE_DATA(false, false);

    private boolean shouldRunBenchmark;
    private boolean dataSetPresent;

    Mode(boolean shouldRunBenchmark, boolean dataSetPresent) {
      this.shouldRunBenchmark = shouldRunBenchmark;
      this.dataSetPresent = dataSetPresent;
    }

    static Mode getMode(boolean shouldRunBenchmark, boolean dataSetPresent) {
      if (shouldRunBenchmark) {
        if (dataSetPresent) {
          return RUN_BENCHMARK;
        } else {
          return GENERATE_DATA_AND_RUN_BENCHMARK;
        }
      } else {
        if (dataSetPresent) {
          return DO_NOTHING;
        } else {
          return GENERATE_DATA;
        }
      }
    }
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
