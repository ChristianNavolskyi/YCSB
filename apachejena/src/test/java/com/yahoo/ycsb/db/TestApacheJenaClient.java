package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.graph.Edge;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestApacheJenaClient {

  private static File directory;
  private static Properties properties;
  private static ApacheJenaClient apacheJenaClient;

  @BeforeClass
  public static void createDirectory() throws IOException, DBException {
    directory = new File(System.getProperty("user.dir"), "test");
    FileUtils.deleteDirectory(directory);
    assertTrue(directory.mkdirs());

    properties = new Properties();
    properties.setProperty("outputdirectory", directory.getAbsolutePath());

    apacheJenaClient = new ApacheJenaClient();
    apacheJenaClient.setProperties(properties);
    apacheJenaClient.init();
  }

  @AfterClass
  public static void removeDirectory() throws IOException {
    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void testDatabaseCreation() {
    assertTrue(directory.list().length > 0);
  }

  // Multiple operations in one test, because the values can not be removed by deleting the folder.
  // The values are kept in the JVM.
  @Test
  public void testDatabaseOperations() {
    String table = "table";

    String edgeAddress = "address";
    String edgeEats = "eats";
    String edgeHasFriend = "hasFriend";

    String max = "Max";
    String moritz = "Moritz";
    String streetName = "Apache Street 10";


    // insert
    Map<String, ByteIterator> values = new HashMap<>();
    values.put(edgeAddress, new StringByteIterator(streetName));
    values.put(edgeEats, new StringByteIterator("Fish"));
    assertEquals(Status.OK, apacheJenaClient.insert(table, max, values));
    values = new HashMap<>();
    values.put(edgeAddress, new StringByteIterator("Apache Street 14"));
    values.put(edgeEats, new StringByteIterator("Fruits"));
    assertEquals(Status.OK, apacheJenaClient.insert(table, moritz, values));
    Map<String, ByteIterator> edgeValues = new HashMap<>();
    edgeValues.put(Edge.LABEL_IDENTIFIER, new StringByteIterator(edgeHasFriend));
    edgeValues.put(Edge.START_IDENTIFIER, new StringByteIterator(max));
    edgeValues.put(Edge.END_IDENTIFIER, new StringByteIterator(moritz));
    assertEquals(Status.OK, apacheJenaClient.insert(Edge.EDGE_IDENTIFIER, "", edgeValues));

    // read
    Set<String> fields = new HashSet<>();
    fields.add(edgeAddress);
    HashMap<String, ByteIterator> result = new HashMap<>();
    assertEquals(Status.OK, apacheJenaClient.read(table, max, fields, result));
    assertEquals(streetName, result.get(edgeAddress).toString());

    // scan
    fields.add(edgeEats);
    fields.add(edgeHasFriend);
    Vector<HashMap<String, ByteIterator>> resultVector = new Vector<>();
    assertEquals(Status.OK, apacheJenaClient.scan(table, max, 10, fields, resultVector));
    assertEquals(2, resultVector.size());

    // update
    String newStreet = "Apache Street 12";
    values.put(edgeAddress, new StringByteIterator(newStreet));
    assertEquals(Status.OK, apacheJenaClient.update(table, max, values));
    result = new HashMap<>();
    assertEquals(Status.OK, apacheJenaClient.read(table, max, fields, result));
    assertEquals(newStreet, result.get(edgeAddress).toString());

    // delete
    assertEquals(Status.OK, apacheJenaClient.delete(table, max));
    result = new HashMap<>();
    assertEquals(Status.NOT_FOUND, apacheJenaClient.read(table, max, fields, result));
  }
}