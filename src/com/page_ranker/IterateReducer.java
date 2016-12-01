package com.page_ranker;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by luqmanarifin on 01/12/16.
 */
public class IterateReducer extends Reducer<LongWritable, Attribute, LongWritable, Attribute> {

  @Override
  protected void reduce(LongWritable key, Iterable<Attribute> values, Context context) throws IOException, InterruptedException {
    double pageRank = 0;
    String followee = "";
    for (Attribute attribute : values) {
      if (attribute.getSize() > 0) {
        followee = attribute.getFollowee().toString();
      }
      pageRank += attribute.getPageRank();
    }
    Attribute attribute = new Attribute(followee, pageRank);
    context.write(new LongWritable(key.get()), attribute);
  }
}
