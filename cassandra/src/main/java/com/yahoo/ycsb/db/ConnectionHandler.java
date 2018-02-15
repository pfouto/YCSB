package com.yahoo.ycsb.db;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

class ConnectionHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void channelRegistered(final ChannelHandlerContext ctx) {

  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) { // (1)
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
      MigrateMessage mm = (MigrateMessage) msg;
    boolean add = CassandraCQLClient.migrateResponses.get(((MigrateMessage) msg).getThread()).add(mm);
    if(!add){
      //TODO handle better
      System.err.println("ERror adding to queue...");
      System.exit(1);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }

}
