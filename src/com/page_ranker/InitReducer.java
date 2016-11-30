package com.page_ranker;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by luqmanarifin on 29/11/16.
 */
public class InitReducer extends Reducer<User, User, User, User> {

  public void reduce(User key, Iterable<User> values, Context context) throws IOException, InterruptedException {
    long following = 0;
    for (User val : values) following++;
    User valueOutput = new User(key.getId(), new BooleanWritable(false), new LongWritable(following), key.getRank());
    for (User val : values) {
      User keyOutput = new User(val.getId(), new BooleanWritable(false), new LongWritable(0), val.getRank());
      context.write(keyOutput, valueOutput);
    }
  }

}
