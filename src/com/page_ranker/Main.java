package com.page_ranker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
      dfs.delete(new Path(basePath + "/iteration" + i + "/intermediate"), true);
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
    job.setMapOutputKeyClass(User.class);
    job.setMapOutputValueClass(User.class);
    job.setOutputKeyClass(User.class);
    job.setOutputValueClass(User.class);

    String inputPath = getRootDirectory() + "user/luqman/dummy"; //"user/twitter";
    String outputPath = getMyDirectory() + "/iteration0/output";
    System.out.println("twitter path: " + inputPath);
    System.out.println("output path: " + outputPath);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.waitForCompletion(true);
  }

  public static void calculate(int iteration) throws Exception {
    Job job = Job.getInstance(conf, PREFIX + "calculate " + iteration);
    job.setJarByClass(Main.class);
    job.setMapperClass(CalculateMapper.class);
    job.setCombinerClass(CalculateReducer.class);
    job.setReducerClass(CalculateReducer.class);
    job.setOutputKeyClass(User.class);
    job.setOutputValueClass(User.class);

    String inputPath = "";
    String outputPath = "";
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.waitForCompletion(true);
  }

  public static void update(int iteration) throws Exception {
    Job job = Job.getInstance(conf, PREFIX + "update " + iteration);
    job.setJarByClass(Main.class);
    job.setMapperClass(UpdateMapper.class);
    job.setCombinerClass(UpdateReducer.class);
    job.setReducerClass(UpdateReducer.class);
    job.setOutputKeyClass(User.class);
    job.setOutputValueClass(User.class);

    String inputPath = "";
    String outputPath = "";
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.waitForCompletion(true);
  }

  public static void finish() throws Exception {
    Job job = Job.getInstance(conf, PREFIX + "finish");
    job.setJarByClass(Main.class);
    job.setMapperClass(FinishMapper.class);
    job.setCombinerClass(FinishReducer.class);
    job.setReducerClass(FinishReducer.class);
    job.setOutputKeyClass(User.class);
    job.setOutputValueClass(User.class);

    String inputPath = "";
    String outputPath = "";
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
	  cleanUp();
    init();
	  /*
    for (int i = 1; i <= ITERATION; i++) {
      calculate(i);
      update(i);
    }
    finish();
    */
  }
}
