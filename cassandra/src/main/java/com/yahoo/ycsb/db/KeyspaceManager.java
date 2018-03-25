package com.yahoo.ycsb.db;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Insert;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.yahoo.ycsb.Client.DO_TRANSACTIONS_PROPERTY;
import static com.yahoo.ycsb.Client.main;

public class KeyspaceManager {

  private static final String LOCAL_LAMBDA_PROPERTY = "cassandra.locallambda";
  private static final String REMOTE_LAMBDA_PROPERTY = "cassandra.remotelambda";
  private static final String LOCAL_KEYSPACES_PROPERTY = "cassandra.localkeyspaces";
  private static final String REMOTE_KEYSPACES_PROPERTY = "cassandra.remotekeyspaces";
  public static final String MIGRATE_PROPERTY = "migrate";

  private static final String MAIN_KEYSPACE_PROPERTY = "cassandra.mainkeyspace";

  private int lblTs = -1;
  private InetAddress lblSrc = InetAddress.getLoopbackAddress();

  private int localLambda;
  private int remoteLambda;

  private String[] localKeyspaces;
  private String[] remoteKeyspaces;
  private String mainKeyspace;

  private int nSequenceOps;
  private int currentSequenceOp;

  private boolean local;
  private String currentKeyspace;

  public String currentDc;

  private static Random r = new Random();

  private boolean running;

  private boolean migrate;

  private List<Map.Entry<String, Long>> allOps;

  KeyspaceManager(Properties properties) {
    localKeyspaces = properties.getProperty(LOCAL_KEYSPACES_PROPERTY).split("\\s+");
    remoteKeyspaces = properties.getProperty(REMOTE_KEYSPACES_PROPERTY).split("\\s+");
    mainKeyspace = properties.getProperty(MAIN_KEYSPACE_PROPERTY);
    migrate = Boolean.valueOf(properties.getProperty(MIGRATE_PROPERTY));

    currentDc = mainKeyspace;

    localLambda = Integer.valueOf(properties.getProperty(LOCAL_LAMBDA_PROPERTY));
    remoteLambda = Integer.valueOf(properties.getProperty(REMOTE_LAMBDA_PROPERTY));

    running = Boolean.valueOf(properties.getProperty(DO_TRANSACTIONS_PROPERTY));

    local = true;
    currentSequenceOp = 0;
    nSequenceOps = getPoisson(true);
    nSequenceOps = r.nextInt(nSequenceOps);
    currentKeyspace = getRandomKeyspace(local);

    allOps = new LinkedList<>();
  }

  public String nextOpKeyspace() {
    if (!running) {
      return mainKeyspace;
    }

    if(remoteLambda == 0){
      return mainKeyspace;
    }

    currentSequenceOp++;

    //next sequence
    if (currentSequenceOp >= nSequenceOps) {
      local = !local;
      currentKeyspace = getRandomKeyspace(local);
      nSequenceOps = getPoisson(local);
      currentSequenceOp = 0;


      if(migrate) {
        //migrate to relevant...
        List<String> possibleDcs = new LinkedList<>();
        if (local) {
          possibleDcs.add(mainKeyspace);
        } else {
          //todo find dcs that replicate new keyspace
          Map<String, String> replication = CassandraCQLClient.clusters.get(currentDc).getMetadata().getKeyspace(currentKeyspace).getReplication();
          for (String dc : replication.keySet()) {
            if (CassandraCQLClient.clusters.containsKey(dc))
              possibleDcs.add(dc);
          }
        }

        try {
          long startTime = System.nanoTime();
          MigrateMessage mm;
          if(CassandraCQLClient.saturn){
            mm = new MigrateMessage(Thread.currentThread().getId(), currentDc, possibleDcs, lblTs, lblSrc, null,
                InetAddress.getLocalHost(), -1, System.currentTimeMillis());
          } else {
            mm = new MigrateMessage(Thread.currentThread().getId(), currentDc, possibleDcs, null,
                InetAddress.getLocalHost(), -1, System.currentTimeMillis());
          }
          ConnectionManager.getConnectionToHigh(InetAddress.getByName(CassandraCQLClient.internals.get(currentDc))).writeAndFlush(mm);

          MigrateMessage take = CassandraCQLClient.migrateResponses.get(Thread.currentThread().getId()).poll(600, TimeUnit.SECONDS);
          if(take == null){
            System.err.println("timeout migrating: " + mm);
            System.out.println("timeout migrating: " + mm);
            System.exit(1);
          }
          long timeTaken = System.nanoTime() - startTime;

          currentDc = take.getPossibleDatacenters().get(0);

          opDone(timeTaken, "m");

        } catch (Exception e) {
          System.err.println("Exception migrating... " + e);
          System.out.println("Exception migrating... " + e);
          System.exit(1);
        }

      } // end migrate if
    } //end next seq if

    return currentKeyspace;
  }

  public void opDone(long nanosTaken, String type) {
    if (!running)
      return;

    allOps.add(new AbstractMap.SimpleImmutableEntry<>((local ? "l" : "r") + "-" + type, nanosTaken));

  }

  public String getCurrentKeyspace() {
    return currentKeyspace;
  }

  public String getMainKeyspace() {
    return mainKeyspace;
  }

  private String getRandomKeyspace(boolean local) {
    if (local)
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

  public List<Map.Entry<String, Long>> getAllOps() {
    return allOps;
  }

  public void addLabel(Insert insertStmt) {
    insertStmt.value("lbl_ts", lblTs);
    insertStmt.value("lbl_src", lblSrc);
  }

  public int getLblTs() {
    return lblTs;
  }

  public void extractNewLabel(Row row) {
    int resTs = row.getInt("lbl_ts");
    InetAddress resSrc = row.getInet("lbl_src");

    if(resTs > lblTs || (resTs == lblTs && resSrc.getHostAddress().compareTo(lblSrc.getHostAddress()) > 0)){
      lblTs = resTs;
      lblSrc = resSrc;
    }
  }
}
