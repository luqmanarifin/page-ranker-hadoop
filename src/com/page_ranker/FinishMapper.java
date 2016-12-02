package com.page_ranker;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by luqmanarifin on 01/12/16.
 */
public class FinishMapper extends Mapper<Object, Text, LongWritable, Attribute> {

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    long id = Long.parseLong(itr.nextToken());
    double pageRank = Double.parseDouble(itr.nextToken());

    Attribute attribute1 = new Attribute(id + "", pageRank);
    context.write(new LongWritable(1), attribute1);
  }
}
