package com.page_ranker;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by luqmanarifin on 01/12/16.
 */
public class Attribute implements Writable {

  private Text followee;
  private DoubleWritable pageRank;

  public Attribute() {
    followee = new Text();
    pageRank = new DoubleWritable(0);
  }

  public Attribute(String param, double pageRank) {
    this.followee = new Text(param);
    this.pageRank = new DoubleWritable(pageRank);
  }

  public List<LongWritable> getFollowing() {
    List<LongWritable> followees = new ArrayList<>();
    String temp = this.followee.toString();
    String[] parsed = temp.split(",");
    for (String str : parsed) {
      try {
        followees.add(new LongWritable(Long.parseLong(str.trim())));
      } catch (Exception e) {

      }
    }
    return followees;
  }

  public double getPageRank() {
    return pageRank.get();
  }

  public void setPageRank(double pageRank) {
    this.pageRank = new DoubleWritable(pageRank);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    followee.write(dataOutput);
    pageRank.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    followee.readFields(dataInput);
    pageRank.readFields(dataInput);
  }

  @Override
  public String toString() {
    return followee.toString() + "\t" + pageRank.get();
  }
}
