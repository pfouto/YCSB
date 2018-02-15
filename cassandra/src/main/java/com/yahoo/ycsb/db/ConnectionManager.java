package com.yahoo.ycsb.db;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConnectionManager {

  private static final int HIGH_PORT = 1805;
  private static final int CLIENT_PORT = 1807;

  private static EventLoopGroup bossGroup;
  private static EventLoopGroup inGroup;
  private static EventLoopGroup outGroup;

  private static ConcurrentMap<InetAddress, Channel> connections;

  public static void init() {
    try {
      bossGroup = new NioEventLoopGroup();
      inGroup = new NioEventLoopGroup();
      outGroup = new NioEventLoopGroup();
      connections = new ConcurrentHashMap<>();

      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, inGroup);
      b.channel(NioServerSocketChannel.class);
      b.childHandler(new ChannelInitializer<SocketChannel>() { // (4)
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
          ch.pipeline().addLast(new MessageDecoder(), new MessageEncoder(), new ConnectionHandler());
          InetAddress connAddress = ch.remoteAddress().getAddress();
          connections.put(connAddress, ch);

          ch.closeFuture().addListener((ChannelFuture cF) -> {
            connections.remove(((InetSocketAddress) cF.channel().remoteAddress()).getAddress());
          });
        }
      });
      b.option(ChannelOption.SO_BACKLOG, 128);
      b.childOption(ChannelOption.SO_KEEPALIVE, true);

      // Bind and start to accept incoming connections.
      ChannelFuture f = b.bind(CLIENT_PORT).sync(); // (7)
      f.channel().closeFuture().addListener((ChannelFuture cF) -> {
        System.err.println("Server socket closed!");
        System.exit(0);
      });
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      outGroup.shutdownGracefully();
      inGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();

      System.exit(0);
    }
  }

  public static Channel getConnectionToHigh(InetAddress addr) throws InterruptedException {
    Channel c = connections.get(addr);
    if (c == null) {
      c = createConnection(addr, HIGH_PORT);
    }
    return c;
  }

  public static ConcurrentMap<InetAddress, Channel> getConnections() {
    return connections;
  }

  private static Channel createConnection(InetAddress addr, int port) throws InterruptedException {
    Bootstrap b = new Bootstrap();
    b.group(outGroup);
    b.channel(NioSocketChannel.class);
    b.option(ChannelOption.SO_KEEPALIVE, true);

    b.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new MessageDecoder(), new MessageEncoder(), new ConnectionHandler());
      }
    });
    ChannelFuture f = b.connect(addr, port).sync();
    InetAddress connAddress = ((InetSocketAddress) f.channel().remoteAddress()).getAddress();
    connections.put(connAddress, f.channel());

    f.channel().closeFuture().addListener((ChannelFuture cF) -> {
      connections.remove(((InetSocketAddress) cF.channel().remoteAddress()).getAddress());
    });
    return f.channel();
  }
}
