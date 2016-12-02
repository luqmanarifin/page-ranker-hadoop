package com.page_ranker;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by luqmanarifin on 01/12/16.
 */
public class IterateMapper extends Mapper<Object, Text, LongWritable, Attribute> {

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    long id = Long.parseLong(itr.nextToken());
    float pageRank = Float.parseFloat(itr.nextToken());
    Attribute attribute = new Attribute(itr.nextToken(), pageRank);
    List<LongWritable> followees = attribute.getFollowing();

    System.out.println(id + " " + pageRank + " " + followees.toString());

    long n = followees.size();
    float addedPageRank = pageRank / (float) n;
    for (LongWritable idFollowee : followees) {
      Attribute attribute2 = new Attribute("", addedPageRank);
      context.write(new LongWritable(idFollowee.get()), attribute2);
    }

    Attribute attribute1 = new Attribute(attribute.getFollowee().toString(), 0);
    context.write(new LongWritable(id), attribute1);
  }
}
