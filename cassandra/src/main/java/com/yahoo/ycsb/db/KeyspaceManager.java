package com.yahoo.ycsb.db;

import java.util.Properties;
import java.util.Random;

import static com.yahoo.ycsb.Client.DO_TRANSACTIONS_PROPERTY;

public class KeyspaceManager {

  private static final String LOCAL_LAMBDA_PROPERTY = "cassandra.locallambda";
  private static final String REMOTE_LAMBDA_PROPERTY = "cassandra.remotelambda";
  private static final String LOCAL_KEYSPACES_PROPERTY = "cassandra.localkeyspaces";
  private static final String REMOTE_KEYSPACES_PROPERTY = "cassandra.remotekeyspaces";

  private static final String MAIN_KEYSPACE_PROPERTY = "cassandra.mainkeyspace";

  private int localLambda;
  private int remoteLambda;

  private String[] localKeyspaces;
  private String[] remoteKeyspaces;
  private String mainKeyspace;


  long localOpsTotalTime;
  int localOpsTotalN;
  long remoteOpsTotalTime;
  int remoteOpsTotalN;

  static long sumLocalOpsTotalTime = 0;
  static int sumLocalOpsTotalN = 0;
  static long sumRemoteOpsTotalTime = 0;
  static int sumRemoteOpsTotalN = 0;

  private int nSequenceOps;
  private int currentSequenceOp;

  private boolean local;
  private String currentKeyspace;

  private static Random r = new Random();

  private boolean running;

  private long lastMillis;

  KeyspaceManager(Properties properties){
    localKeyspaces = properties.getProperty(LOCAL_KEYSPACES_PROPERTY).split("\\s+");
    remoteKeyspaces = properties.getProperty(REMOTE_KEYSPACES_PROPERTY).split("\\s+");
    mainKeyspace = properties.getProperty(MAIN_KEYSPACE_PROPERTY);

    localLambda = Integer.valueOf(properties.getProperty(LOCAL_LAMBDA_PROPERTY));
    remoteLambda = Integer.valueOf(properties.getProperty(REMOTE_LAMBDA_PROPERTY));

    running = Boolean.valueOf(properties.getProperty(DO_TRANSACTIONS_PROPERTY));

    remoteOpsTotalN = 0;
    remoteOpsTotalTime = 0;
    localOpsTotalN = 0;
    localOpsTotalTime = 0;

    /*
    local = true;
    currentKeyspace = getRandomKeyspace(local);
    int poisson = getPoisson(local);
    nSequenceOps = r.nextInt(poisson);
    System.out.println("New sequence: " + nSequenceOps + " " + currentKeyspace);
    currentSequenceOp = -1;*/

    local = false;
    currentSequenceOp = 0;
    nSequenceOps = 0;

    lastMillis = -1;
  }

  public String nextOpKeyspace(){
    if(!running){
      return mainKeyspace;
    }

    currentSequenceOp++;
    if (currentSequenceOp >= nSequenceOps) {

      long newMillis = System.currentTimeMillis();
      if(lastMillis != -1){
        long time = newMillis - lastMillis;
        if(local){
          localOpsTotalTime += time;
          localOpsTotalN += nSequenceOps;
        } else {
          remoteOpsTotalTime += time;
          remoteOpsTotalN += nSequenceOps;
        }
      }

      local = !local;
      currentKeyspace = getRandomKeyspace(local);
      nSequenceOps = getPoisson(local);
      currentSequenceOp = 0;
      //System.out.println("New sequence: " + nSequenceOps + " " + currentKeyspace);
      lastMillis = newMillis;
    }
    return currentKeyspace;
  }

  private String getRandomKeyspace(boolean local) {
    if(local)
      return localKeyspaces[r.nextInt(localKeyspaces.length)];
    else
      return remoteKeyspaces[r.nextInt(remoteKeyspaces.length)];
  }

  private int getPoisson(boolean local) {
    int lambda = local ? localLambda : remoteLambda;
    double L = Math.exp(-lambda);
    double p = 1.0;
    int k = 0;

    do {
      k++;
      p *= Math.random();
    } while (p > L);

    return k - 1;
  }


  public void finish() {
    sumLocalOpsTotalN += localOpsTotalN;
    sumLocalOpsTotalTime += localOpsTotalTime;
    sumRemoteOpsTotalN += remoteOpsTotalN;
    sumRemoteOpsTotalTime += remoteOpsTotalTime;

    //System.out.println("Local ops:" + localOpsTotalN + " time:" + localOpsTotalTime + " ops/s:" + (localOpsTotalN/(localOpsTotalTime/1000)));
    //System.out.println("Remote ops:" + remoteOpsTotalN + " time:" + remoteOpsTotalTime + " ops/s:" + (remoteOpsTotalN/(remoteOpsTotalTime/1000)));
  }

  public static void printOverall(){
    System.out.println("Local ops:" + sumLocalOpsTotalN + " time:" + sumLocalOpsTotalTime + " ops/s:" + (sumLocalOpsTotalN/(sumLocalOpsTotalTime/1000)));
    System.out.println("Remote ops:" + sumRemoteOpsTotalN + " time:" + sumRemoteOpsTotalTime + " ops/s:" + (sumRemoteOpsTotalN/(sumRemoteOpsTotalTime/1000)));

  }
}
