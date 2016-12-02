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
  private Text pageRank;

  public Attribute() {
    followee = new Text();
    pageRank = new Text();
  }

  public Attribute(String param, double pageRank) {
    this.followee = new Text(param);
    this.pageRank = new Text(pageRank + "");
  }

  public Attribute(Text param, Text pageRank) {
    this.followee = param;
    this.pageRank = pageRank;
  }

  public Text getFollowee() {
    return followee;
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

  public long getSize() {
    return getFollowing().size();
  }

  public String getPageRank() {
    return pageRank.toString();
  }

  public void setPageRank(double pageRank) {
    this.pageRank = new Text(pageRank + "");
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    pageRank.write(dataOutput);
    followee.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    pageRank.readFields(dataInput);
    followee.readFields(dataInput);
  }

  @Override
  public String toString() {
    if (followee.getLength() == 0) {
      followee = new Text(",");
    }
    return pageRank.toString() + "\t" + followee.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + followee.hashCode();
    result = prime * result + pageRank.hashCode();
    return result;
  }

}
