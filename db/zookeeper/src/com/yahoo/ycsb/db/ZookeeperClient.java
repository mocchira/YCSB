/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Random;
import java.util.Properties;
import java.io.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;


/**
 * zookeeper client for YCSB framework
 */
public class ZookeeperClient extends DB implements Watcher
{
  public static final int Ok = 0;
  public static final int Error = -1;

  public static final String ROOT_DIR_PROPERTY_DEFAULT = "/ycsb";

  ZooKeeper client = null;
  String dir;
  ArrayList<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  boolean _debug = false;
  boolean init = true;

  public void process(WatchedEvent event){
    //if (event.KeeperState == Watcher.Event.KeeperState.SyncConnected) {
      init = false;
    //}
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  public void init() throws DBException
  {
    String hosts = getProperties().getProperty("hosts");
    if (hosts == null)
    {
      throw new DBException("Required property \"hosts\" missing for ZookeeperClient");
    }

    dir = getProperties().getProperty("zookeeper.dir", ROOT_DIR_PROPERTY_DEFAULT);
    _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
    int retry = 3;
    while(init){
      try {
        client = new ZooKeeper(hosts, 10000, (Watcher)this);
      } catch (IOException e) {
        throw new DBException(e);
      }
      try
      {
        Thread.sleep(1000);
      } catch (InterruptedException e)
      {
      }
      if (--retry == 0) break;
    }
    if (init)
    {
      String errmsg = "Unable to connect to " + hosts;
      System.err.println(errmsg);
      throw new DBException(errmsg);
    }
    try {
      Stat stat = client.exists(dir, false);
      if (stat == null)
        client.create(dir,"".getBytes(),acl,CreateMode.PERSISTENT);
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  public void cleanup() throws DBException
  {
    try {
      client.close();
    } catch (Exception e) {
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public int read(String table, String key, Set<String> fields, HashMap<String, String> result)
  {
    try
    {
      Stat stat = new Stat();
      byte[] buffer = client.getData(dir + "/" + table + "/" + key, false, stat);
      result.put("key", new String(buffer));
    } catch (Exception e)
    {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return Error;
    }

    return Ok;

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public int scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, String>> result)
  {

    try
    {
      List<String> children = client.getChildren(dir + "/" + table,false);
    } catch (Exception e)
    {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return Error;
    }
    
    return Ok;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int update(String table, String key, HashMap<String, String> values)
  {
    try
    {
      String path = dir + "/" + table;
      Stat stat = client.exists(path, false);
      if (stat == null)
        client.create(path,"".getBytes(),acl,CreateMode.PERSISTENT);
      client.setData(path + "/" + key,values.toString().getBytes(),-1);
    } catch (Exception e)
    {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return Error;
    }
    
    return Ok;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int insert(String table, String key, HashMap<String, String> values)
  {

    try
    {
      String path = dir + "/" + table;
      Stat stat = client.exists(path, false);
      if (stat == null)
        client.create(path,"".getBytes(),acl,CreateMode.PERSISTENT);
      client.create(path + "/" + key,values.toString().getBytes(),acl,CreateMode.PERSISTENT);
    } catch (Exception e)
    {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return Error;
    }
    
    return Ok;
  }

  /**
   * Delete a record from the database.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public int delete(String table, String key)
  {
    try
    {
      String path = dir + "/" + table + "/" + key;
      client.delete(path, -1);
    } catch (Exception e)
    {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return Error;
    }
    
    return Ok;
  }

  public static void main(String[] args)
  {
    ZookeeperClient cli = new ZookeeperClient();

    Properties props = new Properties();

    props.setProperty("hosts", args[0]);
    cli.setProperties(props);

    try
    {
      cli.init();
    } catch (Exception e)
    {
      e.printStackTrace();
      System.exit(0);
    }

    HashMap<String, String> vals = new HashMap<String, String>();
    vals.put("age", "57");
    vals.put("middlename", "bradley");
    vals.put("favoritecolor", "blue");
    int res = cli.insert("usertable", "BrianFrankCooper", vals);
    System.out.println("Result of insert: " + res);

    HashMap<String, String> result = new HashMap<String, String>();
    res = cli.read("usertable", "BrianFrankCooper", null, result);
    System.out.println("Result of read: " + res);
    for (String s : result.keySet())
    {
      System.out.println("[" + s + "]=[" + result.get(s) + "]");
    }

    res = cli.delete("usertable", "BrianFrankCooper");
    System.out.println("Result of delete: " + res);
  }

}
