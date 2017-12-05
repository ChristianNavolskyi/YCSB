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
    String key = NODE_IDENTIFIER + node.getId();
    HashMap<String, ByteIterator> values = new HashMap<>();

    values.put("label", new StringByteIterator(node.getLabel()));
    values.put("value", new RandomByteIterator(NODE_BYTE_SIZE));

    return db.insert(GRAPH_TABLE_NAME, key, values).isOk();
  }

  private boolean insertEdge(DB db, Edge edge) {
    String key = EDGE_IDENTIFIER + edge.getId();
    HashMap<String, ByteIterator> values = new HashMap<>();

    values.put("label", new StringByteIterator(edge.getLabel()));
    values.put("start", new NumericByteIterator(edge.getStartNode().getId()));
    values.put("end", new NumericByteIterator(edge.getEndNode().getId()));

    return db.insert(GRAPH_TABLE_NAME, key, values).isOk();
  }

}
