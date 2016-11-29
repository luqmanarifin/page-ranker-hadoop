package com.page_ranker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Main {
  private static final int ITERATION = 3;

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
    Job job = Job.getInstance(conf, "init");
    job.setJarByClass(Main.class);
    job.setMapperClass(InitMapper.class);
    job.setCombinerClass(InitReducer.class);
    job.setReducerClass(InitReducer.class);
    job.setOutputKeyClass(User.class);
    job.setOutputValueClass(User.class);

    FileInputFormat.addInputPath(job, new Path(getRootDirectory() + "/user/twitter/twitter_rv.net"));
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
