package com.page_ranker;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by luqmanarifin on 29/11/16.
 */
public class InitMapper extends Mapper<User, User, User, User> {

  public void map(Object key, Text value, Context context) throws Exception {

  }

}
