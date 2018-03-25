package com.yahoo.ycsb.db;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageDecoder extends ByteToMessageDecoder { // (1)

  private int size = -1;

  @Override
  public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    ByteBufInputStream inputStream = new ByteBufInputStream(in);
    try {

      if (size == -1 && in.readableBytes() >= 4) {
        size = in.readInt();
      }

      if (size != -1 && in.readableBytes() >= size) {
        //code
        int messageCode = in.readInt();
        //from
        InetAddress from = CompactEndpointSerializationHelper.deserialize(inputStream);
        //verb
        int verb = in.readInt();
        //timestamp
        long timestamp = in.readLong();
        switch (messageCode) {
          case MigrateMessage.CODE:
            long thread = inputStream.readLong();
            String sdc = inputStream.readUTF();
            int listSize = in.readInt();
            List<String> possibleDcs = new ArrayList<>(listSize);
            for(int i = 0;i<listSize;i++){
              possibleDcs.add(inputStream.readUTF());
            }
            //clock
            int cSize = in.readInt();
            Map<String, Integer> c = new HashMap<>();
            for(int i = 0;i<cSize;i++){
              String key = inputStream.readUTF();
              int val = inputStream.readInt();
              c.put(key, val);
            }
            int label = in.readInt();
            InetAddress srcSat = CompactEndpointSerializationHelper.deserialize(inputStream);
            out.add(new MigrateMessage(thread, sdc, possibleDcs, label, srcSat, c, from, verb, timestamp));
            break;
          default:
            throw new Exception("unknown/unhandled messageType: " + messageCode);
        }
        size = -1;
      }

    } catch (Exception e) {
      System.err.println("Error Decoding message: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

}
