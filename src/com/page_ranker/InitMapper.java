package com.page_ranker;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by luqmanarifin on 29/11/16.
 */

public class InitMapper extends Mapper<Object, Text, User, User> {

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] arr = value.toString().split("\\s+");
    StringTokenizer itr = new StringTokenizer(value.toString());
    System.out.println(value.toString());
    if (arr.length != 2) {
      throw new IOException();
    } else {
      long idUser = Long.parseLong(itr.nextToken());
      long idFollower = Long.parseLong(itr.nextToken());
      User keyOutput = new User(idFollower, false, 0, 1);
      User valueOutput = new User(idUser, false, 0, 1);
      context.write(keyOutput, valueOutput);
    }
  }

}
