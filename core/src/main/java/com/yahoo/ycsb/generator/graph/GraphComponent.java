package com.yahoo.ycsb.generator.graph;

import com.yahoo.ycsb.ByteIterator;

import java.util.Map;

/**
 * Class to extract common fields and methods from {@link Node}s and {@link Edge}s.
 */
public abstract class GraphComponent {
  static final String ID_IDENTIFIER = "id";
  static final String LABEL_IDENTIFIER = "label";
  private static final String RECREATED = "Recreated";
  private long id;
  private String label;

  GraphComponent(String label, long id) {
    this.id = id;
    this.label = label;
  }

  GraphComponent(long id) {
    this.id = id;
    this.label = RECREATED;
  }

  public final long getId() {
    return id;
  }

  final String getLabel() {
    return label;
  }

  public abstract Map<String, ByteIterator> getHashMap();
}