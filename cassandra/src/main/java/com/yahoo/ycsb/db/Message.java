package com.yahoo.ycsb.db;

import java.net.InetAddress;

public abstract class Message {

  public static final int VERB_WRITE = 0;
  public static final int VERB_READ_REQUEST = 1;
  public static final int VERB_READ_REPLY = 2;

  private final InetAddress from;
  private final int verb;
  private final int code;
  private final long timestamp;

  Message(InetAddress from, int verb, int code, long timestamp)
  {
    this.from = from;
    this.verb = verb;
    this.code = code;
    this.timestamp = timestamp;
  }

  public int getVerb()
  {
    return verb;
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
    return "CODE: " + code + " FROM: " + from + " VERB: " + verb;
  }

}
