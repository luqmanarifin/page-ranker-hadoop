package com.page_ranker;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

  public static class TokenizerMapper
    extends Mapper<Object, Text, User, User>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        Long id = Long.parseLong(itr.nextToken());
        User user1 = new User(id, false, 0, 0);
        User user2 = new User(id, false, 0, 0);
        context.write(user1, user2);
      }
    }
  }

  public static class IntSumReducer
    extends Reducer<User,User,User,User> {
    private IntWritable result = new IntWritable();

    public void reduce(User key, Iterable<User> values,
                       Context context
    ) throws IOException, InterruptedException {
      for (User val : values) {
        User user1 = new User(val.getId().get(), false, 0, 0);
        User user2 = new User(val.getId().get(), false, 0, 0);
        context.write(user1, user2);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Main.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(User.class);
    job.setOutputValueClass(User.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}