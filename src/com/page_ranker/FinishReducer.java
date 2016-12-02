package com.page_ranker;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by luqmanarifin on 01/12/16.
 */
public class FinishReducer extends Reducer<LongWritable, Attribute, LongWritable, Attribute> {

  @Override
  protected void reduce(LongWritable key, Iterable<Attribute> values, Context context) throws IOException, InterruptedException {
    List<Attribute> results = new ArrayList<>();
    for (Attribute attribute : values) {
      Attribute toAdd = new Attribute(attribute.getFollowee().toString(), attribute.getPageRank());
      results.add(toAdd);
      Collections.sort(results, new Comparator<Attribute>() {
        @Override
        public int compare(Attribute a1, Attribute a2) {
          if (a1.getPageRank() > a2.getPageRank()) {
            return -1;
          } else if (a1.getPageRank() < a2.getPageRank()) {
            return 1;
          } else {
            return 0;
          }
        }
      });
      if (results.size() > 5) {
        results.remove(results.size() - 1);
      }
    }
    for (Attribute result : results) {
      Attribute attribute = new Attribute(result.getFollowee().toString(), result.getPageRank());
      context.write(new LongWritable(key.get()), attribute);
    }
  }
}
