package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.Graph;
import com.yahoo.ycsb.generator.graph.GraphGenerator;
import com.yahoo.ycsb.generator.graph.Node;

import java.util.HashMap;
import java.util.Properties;

public class GraphWorkload extends Workload {

  public static final String GRAPH_TABLE_NAME = "graphTable";
  public static final int NODE_BYTE_SIZE = 500;
  public static final String NODE_IDENTIFIER = "Node_";
  public static final String EDGE_IDENTIFIER = "Edge_";
  public static final String GRAPH_ID_IDENTIFIER = "id";
  public static final String GRAPH_LABEL_IDENTIFIER = "label";
  public static final String GRAPH_VALUE_IDENTIFIER = "value";
  public static final String GRAPH_START_IDENTIFIER = "start";
  public static final String GRAPH_END_IDENTIFIER = "end";

  GraphGenerator graphGenerator;

  @Override
  public void init(Properties properties) throws WorkloadException {
    super.init(properties);

    graphGenerator = new GraphGenerator(properties);
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    Graph graph = graphGenerator.nextValue();

    for (Node node : graph.getNodes()) {
      if (!insertNode(db, node)) {
        return false;
      }
    }

    for (Edge edge : graph.getEdges()) {
      if (!insertEdge(db, edge)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    return false;
  }

  private boolean insertNode(DB db, Node node) {
    HashMap<String, ByteIterator> values = new HashMap<>();

    values.put(GRAPH_ID_IDENTIFIER, new StringByteIterator(String.valueOf(node.getId())));
    values.put(GRAPH_LABEL_IDENTIFIER, new StringByteIterator(node.getLabel()));
    values.put(GRAPH_VALUE_IDENTIFIER, new RandomByteIterator(NODE_BYTE_SIZE));

    return db.insert(GRAPH_TABLE_NAME, NODE_IDENTIFIER, values).isOk();
  }

  private boolean insertEdge(DB db, Edge edge) {
    HashMap<String, ByteIterator> values = new HashMap<>();

    values.put(GRAPH_ID_IDENTIFIER, new StringByteIterator(String.valueOf(edge.getId())));
    values.put(GRAPH_LABEL_IDENTIFIER, new StringByteIterator(edge.getLabel()));
    values.put(GRAPH_START_IDENTIFIER, new NumericByteIterator(edge.getStartNode().getId()));
    values.put(GRAPH_END_IDENTIFIER, new NumericByteIterator(edge.getEndNode().getId()));

    return db.insert(GRAPH_TABLE_NAME, EDGE_IDENTIFIER, values).isOk();
  }

}
