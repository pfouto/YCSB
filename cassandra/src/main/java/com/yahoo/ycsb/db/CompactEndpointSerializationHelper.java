package com.yahoo.ycsb.db;

import java.io.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;

/**
 * Taken from org.apache.cassandra.net
 */
public class CompactEndpointSerializationHelper
{
  public static void serialize(InetAddress endpoint, DataOutput out) throws IOException
  {
    if(endpoint != null){
      byte[] buf = endpoint.getAddress();
      out.writeByte(buf.length);
      out.write(buf);
    } else {
      out.writeByte(0);
    }
  }

  public static InetAddress deserialize(DataInput in) throws IOException
  {

    byte[] bytes = new byte[in.readByte()];
    if(bytes.length == 0) {
      return null;
    }
    in.readFully(bytes, 0, bytes.length);
    return InetAddress.getByAddress(bytes);
  }

  public static int serializedSize(InetAddress from)
  {
    if (from instanceof Inet4Address)
      return 1 + 4;
    assert from instanceof Inet6Address;
    return 1 + 16;
  }
}
