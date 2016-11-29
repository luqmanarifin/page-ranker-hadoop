package com.page_ranker;

/**
 * Created by luqmanarifin on 29/11/16.
 */
public class User implements Comparable<User> {

  private long id;
  private boolean isDummy;
  private long following;
  private double rank;

  public User(long id, boolean isDummy, long following, double rank) {
    this.id = id;
    this.isDummy = isDummy;
    this.following = following;
    this.rank = rank;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public boolean isDummy() {
    return isDummy;
  }

  public void setDummy(boolean dummy) {
    isDummy = dummy;
  }

  public long getFollowing() {
    return following;
  }

  public void setFollowing(long following) {
    this.following = following;
  }

  public double getRank() {
    return rank;
  }

  public void setRank(double rank) {
    this.rank = rank;
  }

  @Override
  public int compareTo(User other) {
    if (this.id < other.id) {
      return -1;
    } else if (this.id > other.id) {
      return 1;
    } else {
      return 0;
    }
  }
}
