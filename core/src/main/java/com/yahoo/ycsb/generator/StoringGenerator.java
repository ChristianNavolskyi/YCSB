package com.yahoo.ycsb.generator;

import java.io.File;
import java.io.IOException;

public interface StoringGenerator {

  String getExceptionMessage();

  boolean checkFiles(File directory, File... files) throws IOException;
}
