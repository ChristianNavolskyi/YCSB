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

package com.yahoo.ycsb.db;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.generator.graph.GraphFileReader;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static com.yahoo.ycsb.generator.graph.GraphFileReader.*;
import static com.yahoo.ycsb.workloads.GraphWorkload.*;
import static java.io.File.separatorChar;

/**
 * This "database" creates a file on the given path from properties for each insert call.
 * Is should be used to ensure that the data is the same on multiple benchmark runs with different databases.
 */
public class FileStoreClient extends DB {

  private final GsonBuilder gsonBuilder = new GsonBuilder().registerTypeAdapter(ByteIterator.class, new
      GraphFileReader.ByteIteratorAdapter());
  private final Type valuesType = new TypeToken<Map<String, ByteIterator>>() {}.getType();

  private Gson gson;
  private Map<String, FileWriter> fileWriterMap;
  private String outputDirectory;

  @Override
  public void init() throws DBException {
    Properties properties = getProperties();
    outputDirectory = properties.getProperty(DATA_SET_DIRECTORY_PROPERTY, DATA_SET_DIRECTORY_DEFAULT);

    if (outputDirectory.charAt(outputDirectory.length() - 1) != separatorChar) {
      outputDirectory += separatorChar;
    }

    File directory = new File(outputDirectory);

    if (!directory.exists() && !directory.mkdirs()) {
      throw new DBException("Could not create output directory for files with path: " + outputDirectory);
    }

    gson = gsonBuilder.create();
    fileWriterMap = new HashMap<>();
  }

  @Override
  public void cleanup() throws DBException {
    for (String key : fileWriterMap.keySet()) {
      try {
        fileWriterMap.get(key).close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    super.cleanup();
  }

  /**
   * Reads the file with the name {@code table}.json.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The specified fields in that file, if present.
   * If there is no such file, {@code Status.NOT_FOUND} will be returned.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String filename = getDatabaseFileName(table);

    try (JsonReader jsonReader = getJsonReader(key, filename)) {
      Map<String, ByteIterator> values = gson.fromJson(jsonReader, valuesType);

      for (String field : fields) {
        result.put(field, values.get(field));
      }

      return Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return Status.NOT_FOUND;
  }

  @Override
  public Status scan(String table,
                     String startKey,
                     int recordCount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    String filename = getDatabaseFileName(table);

    try {
      List<String> strings = Files.readAllLines(Paths.get(filename),
          Charset.forName(new FileReader(filename).getEncoding()));

      int start = Integer.parseInt(startKey);
      int lastWantedKey = start + recordCount - 1;
      int lastPossibleKey = getKeyFromKeyString(strings.get(strings.size() - 1));

      int lastKeyToScan = lastPossibleKey > lastWantedKey ? lastWantedKey : lastPossibleKey;

      for (int i = start; i <= lastKeyToScan; i++) {
        JsonReader jsonReader = getJsonReader(String.valueOf(i), filename);
        Map<String, ByteIterator> values = gson.fromJson(jsonReader, valuesType);
        result.add(convertToHashMap(values, fields));
      }

      return Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String filename = getDatabaseFileName(table);

    try (JsonReader jsonReader = getJsonReader(key, filename)) {

      Map<String, ByteIterator> map = gson.fromJson(jsonReader, valuesType);

      for (String valuesKey : values.keySet()) {
        map.put(valuesKey, values.get(valuesKey));
      }

      String updatedEntry = gson.toJson(map, valuesType);
      replaceEntryInFile(updatedEntry, key, filename);

      return Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String output = gson.toJson(values, valuesType);
    String filename = getDatabaseFileName(table);

    try (FileWriter fileWriter = new FileWriter(filename, true)) {
      if (!containsKey(key, table)) {
        writeToFile(key, output, fileWriter);
      }

      return Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    String filename = getDatabaseFileName(table);

    if (new File(filename).delete()) {
      return Status.OK;
    }

    return Status.ERROR;
  }


  private void writeToFile(String key, String output, FileWriter fileWriter) throws IOException {
    fileWriter.write(getKeyString(key));
    fileWriter.write(output);
    fileWriter.write("\n");
    fileWriter.flush();
  }

  private void replaceEntryInFile(String updatedEntry, String key, String filename) throws IOException {
    List<String> fileContents = getLinesOfStringsFromFile(filename);
    String keyString = getKeyString(key);
    FileWriter fileWriter = new FileWriter(filename, false);

    for (String content : fileContents) {
      if (!content.startsWith(keyString)) {
        fileWriter.write(content);
        fileWriter.write("\n");
      } else {
        writeToFile(key, updatedEntry, fileWriter);
      }
    }

    fileWriter.close();
  }

  private boolean containsKey(String key, String table) throws IOException {
    List<String> components = getLinesOfStringsFromFile(getDatabaseFileName(table));
    String keyString = getKeyString(key);

    for (String component : components) {
      if (component.startsWith(keyString)) {
        return true;
      }
    }

    return false;
  }

  private <V> HashMap<String, V> convertToHashMap(Map<String, V> map, Set<String> fields) {
    HashMap<String, V> result = new HashMap<>();

    if (fields != null && fields.size() > 0) {
      for (String field : fields) {
        result.put(field, map.get(field));
      }
    } else {
      result.putAll(map);
    }

    return result;
  }

  private String getDatabaseFileName(String table) {
    return outputDirectory + table + ".json";
  }
}
