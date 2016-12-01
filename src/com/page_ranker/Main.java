package com.page_ranker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {
  private static final int ITERATION = 3;
  private static final String PREFIX = "[luqman ganteng] ";

  private static Configuration conf = new Configuration();

  public static String getRootDirectory() throws Exception {
    FileSystem dfs = FileSystem.get(conf);
    Path now = dfs.getHomeDirectory();
    return (now.getParent().getParent()).toString();
  }

  public static String getMyDirectory() throws Exception {
    FileSystem dfs = FileSystem.get(conf);
    Path now = dfs.getHomeDirectory();
    now = new Path(now.toString()).getParent();
    return (new Path(now.toString() + "/luqman")).toString();
  }

  public static void cleanUp() throws Exception {
    String basePath = getMyDirectory();
    FileSystem dfs = FileSystem.get(conf);

    for (int i = 0; i <= 3; i++) {
      dfs.mkdirs(new Path(basePath + "/iteration" + i));
      dfs.delete(new Path(basePath + "/iteration" + i + "/output"), true);
    }
    dfs.delete(new Path(basePath + "/result"), true);
  }

  public static void init() throws Exception {
    Job job = Job.getInstance(conf, PREFIX + "init");
    job.setJarByClass(Main.class);
    job.setMapperClass(InitMapper.class);
    job.setCombinerClass(InitReducer.class);
    job.setReducerClass(InitReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Attribute.class);

    String inputPath = getRootDirectory() + "user/luqman/dummy"; //"user/twitter";
    String outputPath = getMyDirectory() + "/iteration0/output";
    System.out.println("twitter path: " + inputPath);
    System.out.println("output path: " + outputPath);
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.waitForCompletion(true);
  }

  public static void iterate(int iteration) throws Exception {
    Job job = Job.getInstance(conf, PREFIX + "iterasi " + iteration);
    job.setJarByClass(Main.class);
    job.setMapperClass(IterateMapper.class);
    job.setCombinerClass(IterateReducer.class);
    job.setReducerClass(IterateReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Attribute.class);

    String inputPath = getMyDirectory() + "/iteration" + (iteration - 1) + "/output";
    String outputPath = getMyDirectory() + "/iteration" + (iteration) + "/output";
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.waitForCompletion(true);
  }

  public static void finish() {

  }

  public static void main(String[] args) throws Exception {
    cleanUp();
    init();
    for (int i = 1; i <= ITERATION; i++) {
      iterate(i);
    }
    /*
    finish();
    */
  }
}
