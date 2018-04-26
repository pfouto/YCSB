package com.yahoo.ycsb.db;

import java.net.InetAddress;

public abstract class Message {


  private final InetAddress from;
  private final int code;
  private final long timestamp;

  Message(InetAddress from, int code, long timestamp)
  {
    this.from = from;
    this.code = code;
    this.timestamp = timestamp;
  }


  public InetAddress getFrom()
  {
    return from;
  }

  public int getCode() {
    return code;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString(){
    return "CODE: " + code + " FROM: " + from;
  }

}
