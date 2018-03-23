package com.yahoo.ycsb.db;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MigrateMessage extends Message{

  public final static int CODE = 6;

  private String sourceDc;
  private List<String> possibleDatacenters;
  private Map<String, Integer> clock;
  private long thread;
  private int saturnLabel;
  private InetAddress srcSaturn;

  public MigrateMessage(long thread, String sourceDc, List<String> possibleDatacenters,
                        Map<String, Integer> clock, InetAddress from, int verb, long timestamp) {
    this(thread, sourceDc, possibleDatacenters, -1, null, clock, from, verb, timestamp);
  }

  public MigrateMessage(long thread, String sourceDc, List<String> possibleDatacenters, int labelSaturn,
                        InetAddress srcSaturn, Map<String, Integer> clock, InetAddress from, int verb, long timestamp) {
    super(from, verb, CODE, timestamp);
    this.thread = thread;
    this.sourceDc = sourceDc == null ? "" : sourceDc;
    this.clock = clock == null ? Collections.emptyMap() : clock;
    this.possibleDatacenters = possibleDatacenters;
    this.saturnLabel = labelSaturn;
    this.srcSaturn = srcSaturn;
  }

  public InetAddress getSrcSaturn() {
    return srcSaturn;
  }

  public int getSaturnLabel() {
    return saturnLabel;
  }

  public List<String> getPossibleDatacenters() {
    return possibleDatacenters;
  }

  public String getSourceDc() {
    return sourceDc;
  }

  public long getThread() {
    return thread;
  }

  public Map<String, Integer> getClock() {
    return clock;
  }

  @Override
  public String toString(){
    return super.toString() + " THREAD: " + thread + " SOURCEDC: " + sourceDc
        + " DATACENTERS: " + possibleDatacenters + " CLOCK " + clock + " TSSAT: " + saturnLabel
        + " SRCSAT: " + srcSaturn;
  }
}