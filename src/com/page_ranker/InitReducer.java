package com.page_ranker;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by luqmanarifin on 29/11/16.
 */

public class InitReducer extends Reducer<User, User, User, User> {

  public void reduce(User key, Iterable<User> values, Context context) throws IOException, InterruptedException {
    long following = 0;
    for (User val : values) following++;
    System.out.println("key " + key.getId().get() + " following " + following);
    for (User val : values) {
      User keyOutput = new User(val.getId().get(), false, 0, val.getRank().get());
      User valueOutput = new User(key.getId().get(), false, following, key.getRank().get());
      context.write(keyOutput, valueOutput);
    }
  }

}
