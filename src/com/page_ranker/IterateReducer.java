package com.page_ranker;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * Created by luqmanarifin on 01/12/16.
 */
public class IterateReducer extends Reducer<LongWritable, Attribute, LongWritable, Attribute> {

  @Override
  protected void reduce(LongWritable key, Iterable<Attribute> values, Context context) throws IOException, InterruptedException {
    Text text = new Text();
    double sum = 0;
    for (Attribute attribute : values) {
      List<LongWritable> followees = attribute.getFollowing();
      for (int i = 0; i < followees.size(); i++) {
        String add = followees.get(i).get() + ",";
        text.append(add.getBytes(), 0, add.length());
      }
      sum += attribute.getPageRank();
    }
    Attribute attribute1 = new Attribute(text.toString(), 0.15 + 0.85 * sum);
    context.write(new LongWritable(key.get()), attribute1);
  }
}
