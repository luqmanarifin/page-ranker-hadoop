package com.page_ranker;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by luqmanarifin on 29/11/16.
 */
public class CalculateReducer extends Reducer<User, User, User, User> {

  public void reduce(User key, Iterable<User> values, Context context) throws IOException, InterruptedException {

  }

}
