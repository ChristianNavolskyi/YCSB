package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.graph.*;

import java.util.*;

import static com.yahoo.ycsb.generator.graph.Edge.EDGE_FIELDS_SET;
import static com.yahoo.ycsb.generator.graph.Node.NODE_FIELDS_SET;
import static com.yahoo.ycsb.workloads.CoreWorkload.*;

/**
 * Workload class for graph databases.
 * <p>
 * Every node will have a size of 500 Bytes by default.
 * This can be changed via the {@value NODE_BYTE_SIZE_PROPERTY} parameter.
 * <p>
 * The recordcount property determines how many nodes will be inserted. The total amount of database inserts could
 * be higher due to edges being inserted.
 */
public class GraphWorkload extends Workload {

  private static final int RANDOM_NODE = 0;
  private static final int RANDOM_EDGE = 1;

  private static final String GRAPH_TABLE_NAME = "graphTable";
  private static final String NODE_IDENTIFIER = "Node_";
  private static final String EDGE_IDENTIFIER = "Edge_";

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

  @Override
  public void init(Properties properties) throws WorkloadException {
    super.init(properties);

    graphGenerator = new GraphGenerator(properties);
    discreteGenerator = CoreWorkload.createOperationGenerator(properties);

    nodeByteSize = Integer.parseInt(properties.getProperty(NODE_BYTE_SIZE_PROPERTY, NODE_BYTE_SIZE_DEFAULT));
    maxScanLength = Integer.parseInt(properties.getProperty(MAX_SCAN_LENGTH_PROPERTY,
        MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
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
  public boolean doTransaction(DB db, Object threadstate) {
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
    RandomGraphComponentChooser randomGraphComponentChooser = new RandomGraphComponentChooser().choose();

    Map<String, ByteIterator> map = new HashMap<>();

    db.read(GRAPH_TABLE_NAME,
        randomGraphComponentChooser.key,
        randomGraphComponentChooser.fieldSet,
        map
    );

    printMap(map);
  }

  private void printMap(Map<String, ByteIterator> map) {
    for (String s : map.keySet()) {
      System.out.println("Key: " + s + " Value: " + map.get(s));
    }
  }

  private void doTransactionUpdate(DB db) {
    RandomGraphComponentChooser randomGraphComponentChooser = new RandomGraphComponentChooser();

    Map<String, ByteIterator> map = randomGraphComponentChooser.graphComponent.getHashMap();

    db.update(GRAPH_TABLE_NAME,
        randomGraphComponentChooser.key,
        map
    );

    printMap(map);
  }

  private void doTransactionScan(DB db) {
    RandomGraphComponentChooser randomGraphComponentChooser = new RandomGraphComponentChooser().choose();

    Vector<HashMap<String, ByteIterator>> hashMaps = new Vector<>();

    db.scan(GRAPH_TABLE_NAME,
        randomGraphComponentChooser.key,
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
    String key = NODE_IDENTIFIER + node.getId();
    Map<String, ByteIterator> values = new HashMap<>();

    db.read(GRAPH_TABLE_NAME, key, NODE_FIELDS_SET, values);

    System.out.println("old");
    printMap(values);

    values = node.getHashMap();

    System.out.println("new");
    printMap(values);

    db.update(GRAPH_TABLE_NAME, key, values);
  }

  private Edge chooseRandomEdge() {
    Graph graph = graphGenerator.lastValue();
    int position = random.nextInt(graph.getEdges().size());

    return graph.getEdges().get(position);
  }

  private Node chooseRandomNode() {
    Graph graph = graphGenerator.lastValue();
    int position = random.nextInt(graph.getNodes().size());

    return graph.getNodes().get(position);
  }

  private int randomNodeOrEdge() {
    return random.nextInt(2);
  }

  private boolean insertNode(DB db, Node node) {
    String key = NODE_IDENTIFIER + node.getId();
    Map<String, ByteIterator> values = node.getHashMap();

    System.out.println("Node: " + key);
    printMap(values);

    return db.insert(GRAPH_TABLE_NAME, key, values).isOk();
  }

  private boolean insertEdge(DB db, Edge edge) {
    String key = EDGE_IDENTIFIER + edge.getId();
    Map<String, ByteIterator> values = edge.getHashMap();

    System.out.println("Edge: " + key);
    printMap(values);

    return db.insert(GRAPH_TABLE_NAME, key, values).isOk();
  }

  private class RandomGraphComponentChooser {
    private GraphComponent graphComponent;
    private Set<String> fieldSet;
    private String key;

    RandomGraphComponentChooser() {
      this.fieldSet = new HashSet<>();
      this.key = "";

      choose();
    }

    private RandomGraphComponentChooser choose() {
      switch (randomNodeOrEdge()) {
      case RANDOM_NODE:
        graphComponent = chooseRandomNode();
        key = NODE_IDENTIFIER + graphComponent.getId();
        fieldSet = NODE_FIELDS_SET;
        break;
      case RANDOM_EDGE:
        graphComponent = chooseRandomEdge();
        key = EDGE_IDENTIFIER + graphComponent.getId();
        fieldSet = EDGE_FIELDS_SET;
        break;
      default:
        break;
      }
      return this;
    }
  }
}
