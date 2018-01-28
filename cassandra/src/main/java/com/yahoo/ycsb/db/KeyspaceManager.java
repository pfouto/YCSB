package com.yahoo.ycsb.db;

import java.util.Properties;
import java.util.Random;

public class KeyspaceManager {

  private static final String LOCAL_LAMBDA_PROPERTY = "cassandra.locallambda";
  private static final String REMOTE_LAMBDA_PROPERTY = "cassandra.remotelambda";
  private static final String LOCAL_KEYSPACES_PROPERTY = "cassandra.localkeyspaces";
  private static final String REMOTE_KEYSPACES_PROPERTY = "cassandra.remotekeyspaces";

  private int localLambda;
  private int remoteLambda;

  private String[] localKeyspaces;
  private String[] remoteKeyspaces;


  private int nSequenceOps;
  private int currentSequenceOp;

  private boolean local;
  private String currentKeyspace;

  private static Random r = new Random();

  KeyspaceManager(Properties properties){
    localKeyspaces = properties.getProperty(LOCAL_KEYSPACES_PROPERTY).split("\\s+");
    remoteKeyspaces = properties.getProperty(REMOTE_KEYSPACES_PROPERTY).split("\\s+");
    localLambda = Integer.valueOf(properties.getProperty(LOCAL_LAMBDA_PROPERTY));
    remoteLambda = Integer.valueOf(properties.getProperty(REMOTE_LAMBDA_PROPERTY));

    local = true;
    //noinspection ConstantConditions
    currentKeyspace = getRandomKeyspace(local);
    int poisson = getPoisson(local);
    nSequenceOps = r.nextInt(poisson);
    currentSequenceOp = -1;
  }

  public String nextOpKeyspace(){
    currentSequenceOp++;
    if (currentSequenceOp >= nSequenceOps) {
      local = !local;
      currentKeyspace = getRandomKeyspace(local);
      nSequenceOps = getPoisson(local);
      currentSequenceOp = 0;
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

}
