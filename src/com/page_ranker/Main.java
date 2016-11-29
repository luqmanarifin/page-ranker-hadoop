package com.page_ranker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class Main {
  private static final int ITERATION = 3;

  private static Configuration conf = new Configuration();
  private static Job job = null;

  public static void cleanUp() {

  }

  public static void init() {

  }

  public static void calculate(int iteration) {

  }

  public static void update(int iteration) {

  }

  public static void finish() {

  }

  public static void main(String[] args) throws Exception {
	  cleanUp();
    init();
	  for (int i = 1; i <= ITERATION; i++) {
      calculate(i);
      update(i);
    }
    finish();
  }
}
