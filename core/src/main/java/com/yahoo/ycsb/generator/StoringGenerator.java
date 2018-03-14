package com.yahoo.ycsb.generator;

import java.io.File;
import java.io.IOException;

public abstract class StoringGenerator<V> extends Generator<V> {

  protected static boolean checkDataPresentAndCleanIfSomeMissing(String className, File... files) {
    if (checkIfSomeFilesAbsent(files)) {
      printIfFileMissing(className, files);
      deleteAllFiles(className, files);

      return false;
    }

    return checkAllFilesPresent(files);
  }

  private static boolean checkIfSomeFilesAbsent(File... files) {
    boolean allFilesPresent = checkAllFilesPresent(files);
    boolean allFilesAbsent = checkAllFilesAbsent(files);

    return !allFilesPresent && !allFilesAbsent;
  }

  private static boolean checkAllFilesAbsent(File... files) {
    boolean allFilesAbsent = true;

    for (File file : files) {
      allFilesAbsent = allFilesAbsent && !file.exists();
    }

    return allFilesAbsent;
  }

  private static boolean checkAllFilesPresent(File... files) {
    boolean allFilesPresent = true;

    for (File file : files) {
      allFilesPresent = allFilesPresent && file.exists();
    }

    return allFilesPresent;
  }

  private static void deleteAllFiles(String className, File... files) {
    for (File file : files) {
      if (file.delete()) {
        System.out.println(className + " deleted " + file.getName() + ".");
      }
    }
  }

  private static void printIfFileMissing(String className, File... files) {
    for (File file : files) {
      if (!file.exists()) {
        System.out.println(className + " " + file.getName() + " is missing.");
      }
    }
  }

  protected abstract String getExceptionMessage();

  protected abstract boolean checkFiles(File directory, File... files) throws IOException;
}
