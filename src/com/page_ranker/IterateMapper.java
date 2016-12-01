package com.page_ranker;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Attr;

import java.io.IOException;
import java.util.List;

/**
 * Created by luqmanarifin on 01/12/16.
 */
public class IterateMapper extends Mapper<LongWritable, Attribute, LongWritable, Attribute> {

  @Override
  protected void map(LongWritable key, Attribute value, Context context) throws IOException, InterruptedException {
    List<LongWritable> followees = value.getFollowing();
    long n = followees.size();
    for (LongWritable idFollowee : followees) {
      double pageRank = value.getPageRank() / n;
      Attribute attribute = new Attribute("", pageRank);
      context.write(new LongWritable(idFollowee.get()), attribute);
    }
    Attribute attribute1 = new Attribute(value.getFollowee().toString(), 0);
    context.write(new LongWritable(key.get()), attribute1);
  }
}
