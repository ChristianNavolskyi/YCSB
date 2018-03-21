package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.graph.Edge;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.tdb.TDBFactory;

import java.io.File;
import java.util.*;

/**
 * Apache Jena TDB client for YCSB.
 */
public class ApacheJenaClient extends DB {

  private final String outputDirectoryProperty = "outputdirectory";
  private final String outputDirectoryDefault = new File(System.getProperty("user.dir"),
      "database").getAbsolutePath();
  private Dataset dataset;

  @Override
  public void init() throws DBException {
    super.init();

    Properties properties = getProperties();
    String outputDirectory = properties.getProperty(outputDirectoryProperty, outputDirectoryDefault);

    dataset = TDBFactory.createDataset(outputDirectory);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    dataset.begin(ReadWrite.READ);

    try {
      Model model = dataset.getDefaultModel();

      Resource resource = model.createResource(key);

      for (String field : fields) {
        Property property = model.createProperty(field);

        if (model.contains(resource, property)) {
          String value = resource.getProperty(property).getObject().toString();
          result.put(field, new StringByteIterator(value));
        }
      }

      if (result.isEmpty()) {
        return Status.NOT_FOUND;
      }

    } finally {
      dataset.end();
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    dataset.begin(ReadWrite.READ);

    try {
      Model model = dataset.getDefaultModel();

      Resource resource = model.getResource(startkey);

      scanFieldsRecursively(model, resource, recordcount, fields, result);
    } finally {
      dataset.end();
    }

    if (result.size() == 0) {
      return Status.NOT_FOUND;
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    dataset.begin(ReadWrite.WRITE);

    try {
      Model model = dataset.getDefaultModel();
      Resource resource = model.getResource(key);

      for (String field : values.keySet()) {
        Property property = model.getProperty(field);

        if (model.contains(resource, property)) {
          resource.removeAll(property);
          resource.addProperty(property, values.get(field).toString());
        }
      }

      dataset.commit();
    } finally {
      dataset.end();
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    dataset.begin(ReadWrite.WRITE);

    try {
      Model model = dataset.getDefaultModel();

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        Resource startNode = model.createResource(values.get(Edge.START_IDENTIFIER).toString());
        Property property = model.createProperty(values.get(Edge.LABEL_IDENTIFIER).toString());
        Resource endNode = model.createResource(values.get(Edge.END_IDENTIFIER).toString());

        model.add(startNode, property, endNode);

      } else {
        List<Statement> statements = new ArrayList<>();

        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          Statement statement = model.createStatement(
              model.createResource(key),
              model.createProperty(entry.getKey()),
              model.createResource(entry.getValue().toString()));

          statements.add(statement);
        }

        if (statements.isEmpty()) {
          return Status.ERROR;
        }

        model.add(statements);
      }

      dataset.commit();
    } finally {
      dataset.end();
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    dataset.begin(ReadWrite.WRITE);

    try {
      Model model = dataset.getDefaultModel();

      Resource resource = model.getResource(key);

      model.removeAll(resource, null, null);
      model.removeAll(null, null, resource);

      dataset.commit();
    } finally {
      dataset.end();
    }

    return Status.OK;
  }

  private void scanFieldsRecursively(Model model,
                                     Resource resource,
                                     int recordcount,
                                     Set<String> fields,
                                     Vector<HashMap<String, ByteIterator>> result) {
    if (recordcount > 0) {
      HashMap<String, ByteIterator> values = new HashMap<>();

      for (String field : fields) {
        Property property = model.createProperty(field);

        if (model.contains(resource, property)) {
          Resource nextResource = resource.getProperty(property).getResource();
          values.put(field, new StringByteIterator(nextResource.toString()));
          scanFieldsRecursively(model, nextResource, --recordcount, fields, result);
        }
      }

      if (!values.isEmpty()) {
        result.add(values);
      }
    }
  }
}