package com.page_ranker;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by luqmanarifin on 29/11/16.
 */
public class User implements WritableComparable<User> {

  private LongWritable id;
  private BooleanWritable isDummy;
  private LongWritable following;
  private DoubleWritable rank;

  public User() {
    this.id = new LongWritable(0);
    this.isDummy = new BooleanWritable(true);
    this.following = new LongWritable(0);
    this.rank = new DoubleWritable(0);
  }

  public User(long id, boolean isDummy, long following, double rank) {
    this.id = new LongWritable(id);
    this.isDummy = new BooleanWritable(isDummy);
    this.following = new LongWritable(following);
    this.rank = new DoubleWritable(rank);
  }

  public User(LongWritable id, BooleanWritable isDummy, LongWritable following, DoubleWritable rank) {
    this.id = id;
    this.isDummy = isDummy;
    this.following = following;
    this.rank = rank;
  }

  public LongWritable getId() {
    return id;
  }

  public void setId(LongWritable id) {
    this.id = id;
  }

  public BooleanWritable getIsDummy() {
    return isDummy;
  }

  public void setIsDummy(BooleanWritable isDummy) {
    this.isDummy = isDummy;
  }

  public LongWritable getFollowing() {
    return following;
  }

  public void setFollowing(LongWritable following) {
    this.following = following;
  }

  public DoubleWritable getRank() {
    return rank;
  }

  public void setRank(DoubleWritable rank) {
    this.rank = rank;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id.get());
    out.writeLong(following.get());
    out.writeDouble(rank.get());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id.set(in.readLong());
    following.set(in.readLong());
    rank.set(in.readDouble());
    isDummy.set(false);
  }

  @Override
  public int compareTo(User other) {
    if (this.id.get() < other.id.get()) {
      return -1;
    } else if (this.id.get() > other.id.get()) {
      return 1;
    } else {
      return 0;
    }
  }
}
