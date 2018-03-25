/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 * <p>
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package com.yahoo.ycsb.db;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.yahoo.ycsb.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.yahoo.ycsb.Client.DO_TRANSACTIONS_PROPERTY;

/**
 * Cassandra 2.x CQL client.
 * <p>
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

  public static final String YCSB_KEY = "y_id";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String DCS_PROPERTY = "dcs";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";

  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  public static final String MAX_CONNECTIONS_PROPERTY = "cassandra.maxconnections";
  public static final String CORE_CONNECTIONS_PROPERTY = "cassandra.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY = "cassandra.connecttimeoutmillis";
  public static final String READ_TIMEOUT_MILLIS_PROPERTY = "cassandra.readtimeoutmillis";

  public static final String TRACING_PROPERTY = "cassandra.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = "false";

  public static final String SATURN_PROPERTY = "saturn";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static final AtomicInteger timeouts = new AtomicInteger(0);

  private static boolean debug = false;

  private static boolean trace = false;


  public static boolean saturn;

  private static long startTime;
  private static long endTime = 0;

  private static Map<Long, KeyspaceManager> keyspaceManagerMap = new ConcurrentHashMap<>();

  public static Map<String, Cluster> clusters = null;
  private static Map<String, Session> sessions = null;
  public static Map<String, String> internals = null;
  public static Map<Long, BlockingQueue<MigrateMessage>> migrateResponses = new HashMap<>();

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      keyspaceManagerMap.put(Thread.currentThread().getId(), new KeyspaceManager(getProperties()));
      migrateResponses.put(Thread.currentThread().getId(), new LinkedBlockingQueue<>());

      // Check if the cluster has already been initialized
      if (clusters != null) {
        return;
      }

      try {

        System.err.println("Setting up connections...");

        ConnectionManager.init();

        clusters = new ConcurrentHashMap<>();
        sessions = new ConcurrentHashMap<>();
        internals = new ConcurrentHashMap<>();

        debug =
            Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
        trace = Boolean.valueOf(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));
        saturn = Boolean.valueOf(getProperties().getProperty(SATURN_PROPERTY));

        String dcs = getProperties().getProperty(DCS_PROPERTY);
        String[] dcArray = dcs.split(",");

        for (String dc : dcArray) {

          String internalAddress = getProperties().getProperty("internal." + dc);
          String nodeAddresses = getProperties().getProperty("hosts." + dc);
          String[] nodeArray = nodeAddresses.split(",");

          if (!Boolean.valueOf(getProperties().getProperty(KeyspaceManager.MIGRATE_PROPERTY)) &&
              !dc.equals(keyspaceManagerMap.get(Thread.currentThread().getId()).currentDc)) {
            continue;
          }

          internals.put(dc, internalAddress);

          String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

          String username = getProperties().getProperty(USERNAME_PROPERTY);
          String password = getProperties().getProperty(PASSWORD_PROPERTY);

          readConsistencyLevel = ConsistencyLevel.valueOf(
              getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
                  READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
          writeConsistencyLevel = ConsistencyLevel.valueOf(
              getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
                  WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

          Cluster cluster;
          if ((username != null) && !username.isEmpty()) {
            cluster = Cluster.builder().withCredentials(username, password)
                .withPort(Integer.valueOf(port)).addContactPoints(nodeArray).build();
          } else {
            cluster = Cluster.builder().withPort(Integer.valueOf(port))
                .addContactPoints(nodeArray).build();
          }

          String maxConnections = getProperties().getProperty(
              MAX_CONNECTIONS_PROPERTY);
          if (maxConnections != null) {
            cluster.getConfiguration().getPoolingOptions()
                .setMaxConnectionsPerHost(HostDistance.LOCAL,
                    Integer.valueOf(maxConnections));
          }

          String coreConnections = getProperties().getProperty(
              CORE_CONNECTIONS_PROPERTY);
          if (coreConnections != null) {
            cluster.getConfiguration().getPoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.LOCAL,
                    Integer.valueOf(coreConnections));
          }

          String connectTimoutMillis = getProperties().getProperty(
              CONNECT_TIMEOUT_MILLIS_PROPERTY);
          if (connectTimoutMillis != null) {
            cluster.getConfiguration().getSocketOptions()
                .setConnectTimeoutMillis(Integer.valueOf(connectTimoutMillis));
          }

          String readTimoutMillis = getProperties().getProperty(
              READ_TIMEOUT_MILLIS_PROPERTY);
          if (readTimoutMillis != null) {
            cluster.getConfiguration().getSocketOptions()
                .setReadTimeoutMillis(Integer.valueOf(readTimoutMillis));
          }

          Metadata metadata = cluster.getMetadata();
          /*System.err.printf("Connected to cluster: %s:%s\n",
              metadata.getClusterName(), dc);*/
          //System.err.println(metadata.getKeyspace("euw").getReplication());
        /*
        for (Host discoveredHost : metadata.getAllHosts()) {
          System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
              discoveredHost.getDatacenter(), discoveredHost.getAddress(),
              discoveredHost.getRack());
        }*/

          Session session = cluster.connect();

          clusters.put(dc, cluster);
          sessions.put(dc, session);
        }

      } catch (Exception e) {
        throw new DBException(e);
      }
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      startTime = System.currentTimeMillis();
      System.err.println("Starting time count...");
    } // synchronized
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {

    synchronized (INIT_COUNT) {

      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        for (Session s : sessions.values()) {
          s.close();
        }

        for (Cluster c : clusters.values()) {
          c.close();
        }

        if (endTime == 0) {
          endTime = System.currentTimeMillis();
          System.err.println("Stopped time count");
        }

        keyspaceManagerMap.values().forEach(k -> System.err.print(k.getLblTs() + "\t"));
        System.err.println();

        if (Boolean.valueOf(getProperties().getProperty(DO_TRANSACTIONS_PROPERTY))) {
          int opCount = 0;
          for (Map.Entry<Long, KeyspaceManager> entry : keyspaceManagerMap.entrySet()) {
            List<Map.Entry<String, Long>> allOps = entry.getValue().getAllOps();
            System.out.println("[Client] " + entry.getKey() + " " + allOps.size());
            opCount += allOps.size();
            for (Map.Entry<String, Long> e : allOps) {
              System.out.println(e.getKey() + ":" + e.getValue());
            }
          }
          System.out.println("[Timeouts] " + timeouts.get());
          long runtime = endTime - startTime;
          double throughput = 1000.0 * (opCount) / (runtime);
          System.out.println("[OVERALL], RunTime(ms), " + runtime);
          System.out.println("[OVERALL], Throughput(ops/sec), " + throughput);
        }


      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(
            String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {

    KeyspaceManager keyspaceManager = keyspaceManagerMap.get(Thread.currentThread().getId());
    String keyspace = keyspaceManager.nextOpKeyspace();
    try {
      Statement stmt;
      Select.Builder selectBuilder;

      if (fields == null) {
        selectBuilder = QueryBuilder.select().all();
      } else {
        selectBuilder = QueryBuilder.select();
        for (String col : fields) {
          ((Select.Selection) selectBuilder).column(col);
        }
      }

      stmt = selectBuilder.from(keyspace, table).where(QueryBuilder.eq(YCSB_KEY, key))
          .limit(1);
      stmt.setConsistencyLevel(readConsistencyLevel);

      if (debug) {
        System.out.println(stmt.toString());
      }
      if (trace) {
        stmt.enableTracing();
      }

      long startTime = System.nanoTime();
      ResultSet rs = sessions.get(keyspaceManager.currentDc).execute(stmt);
      long timeTaken = System.nanoTime() - startTime;


      if (rs.isExhausted()) {
        System.err.println("Not found");
        System.exit(1);
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      Row row = rs.one();
      ColumnDefinitions cd = row.getColumnDefinitions();

      if(saturn){
        keyspaceManager.extractNewLabel(row);
      }


      for (ColumnDefinitions.Definition def : cd) {
        ByteBuffer val = row.getBytesUnsafe(def.getName());
        if (val != null) {
          result.put(def.getName(), new ByteArrayByteIterator(val.array()));
        } else {
          result.put(def.getName(), null);
        }
      }

      keyspaceManager.opDone(timeTaken, "r");

      return Status.OK;

    } catch (ReadTimeoutException | NoHostAvailableException e) {
      timeouts.incrementAndGet();
      System.err.println("[" + keyspaceManager.getMainKeyspace() + " -> " + keyspaceManager.getCurrentKeyspace() + "] " + "Timeout reading key: " + e);
      System.exit(1);
      return Status.ERROR;
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * <p>
   * Cassandra CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    KeyspaceManager keyspaceManager = keyspaceManagerMap.get(Thread.currentThread().getId());
    String keyspace = keyspaceManager.nextOpKeyspace();

    try {
      Statement stmt;
      Select.Builder selectBuilder;

      if (fields == null) {
        selectBuilder = QueryBuilder.select().all();
      } else {
        selectBuilder = QueryBuilder.select();
        for (String col : fields) {
          ((Select.Selection) selectBuilder).column(col);
        }
      }

      stmt = selectBuilder.from(keyspace, table);

      // The statement builder is not setup right for tokens.
      // So, we need to build it manually.
      String initialStmt = stmt.toString();
      StringBuilder scanStmt = new StringBuilder();
      scanStmt.append(initialStmt.substring(0, initialStmt.length() - 1));
      scanStmt.append(" WHERE ");
      scanStmt.append(QueryBuilder.token(YCSB_KEY));
      scanStmt.append(" >= ");
      scanStmt.append("token('");
      scanStmt.append(startkey);
      scanStmt.append("')");
      scanStmt.append(" LIMIT ");
      scanStmt.append(recordcount);

      stmt = new SimpleStatement(scanStmt.toString());
      stmt.setConsistencyLevel(readConsistencyLevel);

      if (debug) {
        System.out.println(stmt.toString());
      }
      if (trace) {
        stmt.enableTracing();
      }

      long startTime = System.nanoTime();
      ResultSet rs = sessions.get(keyspaceManager.currentDc).execute(stmt);
      long timeTaken = System.nanoTime() - startTime;

      HashMap<String, ByteIterator> tuple;
      while (!rs.isExhausted()) {
        Row row = rs.one();
        tuple = new HashMap<>();

        ColumnDefinitions cd = row.getColumnDefinitions();

        for (ColumnDefinitions.Definition def : cd) {
          ByteBuffer val = row.getBytesUnsafe(def.getName());
          if (val != null) {
            tuple.put(def.getName(), new ByteArrayByteIterator(val.array()));
          } else {
            tuple.put(def.getName(), null);
          }
        }

        result.add(tuple);
      }
      keyspaceManager.opDone(timeTaken, "s");

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error scanning with startkey: " + startkey);
      System.exit(1);
      return Status.ERROR;
    }

  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    // Insert and updates provide the same functionality
    return insert(table, key, values);
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {

    KeyspaceManager keyspaceManager = keyspaceManagerMap.get(Thread.currentThread().getId());
    String keyspace = keyspaceManager.nextOpKeyspace();

    try {
      Insert insertStmt = QueryBuilder.insertInto(keyspace, table);

      // Add key
      insertStmt.value(YCSB_KEY, key);

      // Add fields
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        Object value;
        ByteIterator byteIterator = entry.getValue();
        value = byteIterator.toString();

        insertStmt.value(entry.getKey(), value);
      }

      if(saturn){
        keyspaceManager.addLabel(insertStmt);
      }

      insertStmt.setConsistencyLevel(writeConsistencyLevel);

      if (debug) {
        System.out.println(insertStmt.toString());
      }
      if (trace) {
        insertStmt.enableTracing();
      }

      long startTime = System.nanoTime();
      ResultSet execute = sessions.get(keyspaceManager.currentDc).execute(insertStmt);
      long timeTaken = System.nanoTime() - startTime;

      if(saturn){
        keyspaceManager.extractNewLabel(execute.one());
      }


      keyspaceManager.opDone(timeTaken, "i");

      return Status.OK;
    } catch (WriteTimeoutException | NoHostAvailableException e) {
      System.err.println("[" + keyspaceManager.getMainKeyspace() + " -> " + keyspaceManager.getCurrentKeyspace() + "] " + "Timeout writing key: " + e);
      timeouts.incrementAndGet();
      return Status.ERROR;
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }

    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {

    KeyspaceManager keyspaceManager = keyspaceManagerMap.get(Thread.currentThread().getId());
    String keyspace = keyspaceManager.nextOpKeyspace();

    try {
      Statement stmt;

      stmt = QueryBuilder.delete().from(keyspace, table)
          .where(QueryBuilder.eq(YCSB_KEY, key));
      stmt.setConsistencyLevel(writeConsistencyLevel);

      if (debug) {
        System.out.println(stmt.toString());
      }
      if (trace) {
        stmt.enableTracing();
      }

      long startTime = System.nanoTime();
      sessions.get(keyspaceManager.currentDc).execute(stmt);
      long timeTaken = System.nanoTime() - startTime;

      keyspaceManager.opDone(timeTaken, "d");
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error deleting key: " + key);
      System.exit(1);
    }

    return Status.ERROR;
  }

}
