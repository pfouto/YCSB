package com.yahoo.ycsb.db;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import java.util.Map;

public class MessageEncoder extends ChannelOutboundHandlerAdapter {

  @Override
  public void write(ChannelHandlerContext ctx, Object o, ChannelPromise promise) {
    ByteBuf out = null;
    try {
      if (!(o instanceof MigrateMessage)) {
        System.err.println("Unsupported object received in encoder: " + o);
        return;
      }

      out = ctx.alloc().buffer();
      ByteBufOutputStream outStream = new ByteBufOutputStream(out);
      out.writerIndex(4);
      Message m = (Message) o;

      //code
      outStream.writeInt(m.getCode());
      //from
      CompactEndpointSerializationHelper.serialize(m.getFrom(), outStream);
      //verb
      outStream.writeInt(m.getVerb());
      //timestamp
      outStream.writeLong(m.getTimestamp());
      //specific

      if (o instanceof MigrateMessage){
        MigrateMessage mm = (MigrateMessage) o;
        outStream.writeLong(mm.getThread());
        outStream.writeUTF(mm.getSourceDc());
        outStream.writeInt(mm.getPossibleDatacenters().size());
        for(String dc : mm.getPossibleDatacenters()){
          outStream.writeUTF(dc);
        }
        //Clock
        Map<String, Integer> clock = mm.getClock();
        outStream.writeInt(clock.size());
        for (Map.Entry<String, Integer> clockEntry : clock.entrySet()) {
          outStream.writeUTF(clockEntry.getKey());
          outStream.writeInt(clockEntry.getValue());
        }
        //outStream.writeInt(mm.getSaturnLabel());
        //CompactEndpointSerializationHelper.serialize(mm.getSrcSaturn(), outStream);
      }

      //write size
      int index = out.writerIndex();
      out.writerIndex(0);
      out.writeInt(index - 4);
      out.writerIndex(index);
      ctx.write(out, promise);
    } catch (Exception e) {
      if (out != null && out.refCnt() > 0)
        out.release(out.refCnt());
      promise.tryFailure(e);
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
    }
  }

}

