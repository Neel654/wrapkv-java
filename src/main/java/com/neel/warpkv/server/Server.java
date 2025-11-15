package com.neel.warpkv.server;

import com.neel.warpkv.storage.KvStore;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Server {

  private static final String VERSION = "warpkv 0.1.0";

  public static void main(String[] args) throws Exception {
    // Port and data dir (env/prop with sane defaults)
    int port = Integer.getInteger("warpkv.port",
        Integer.parseInt(System.getenv().getOrDefault("WARPKV_PORT", "8080")));
    String dataProp = System.getProperty("warpkv.data", System.getenv().getOrDefault("WARPKV_DATA", "data"));
    Path dataDir = Path.of(dataProp).toAbsolutePath();
    Files.createDirectories(dataDir);

    final KvStore store = new KvStore(dataDir);
    final int finalPort = port;
    final Path finalDataDir = dataDir;

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try { store.close(); } catch (Exception ignored) {}
      System.out.println("WarpKV shutdown complete.");
    }));

    System.out.printf("🚀 WarpKV %s listening on :%d (data dir: %s)%n", VERSION, finalPort, finalDataDir);
    System.out.printf("metrics: get=%d put=%d del=%d%n",
        store.getCount.get(), store.putCount.get(), store.delCount.get());

    EventLoopGroup boss = new NioEventLoopGroup(1);
    EventLoopGroup worker = new NioEventLoopGroup();

    try {
      ServerBootstrap bootstrap = new ServerBootstrap()
          .group(boss, worker)
          .channel(NioServerSocketChannel.class)
          .childOption(ChannelOption.SO_REUSEADDR, true)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override protected void initChannel(SocketChannel ch) {
              ChannelPipeline p = ch.pipeline();
              p.addLast(new HttpServerCodec());
              p.addLast(new HttpObjectAggregator(1 << 20)); // 1 MiB
              p.addLast(new SimpleChannelInboundHandler<FullHttpRequest>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
                  FullHttpResponse res;
                  try {
                    String uri = req.uri();

                    // CORS preflight
                    if (req.method() == HttpMethod.OPTIONS) {
                      res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
                      addCors(res);
                      ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
                      return;
                    }

                    if (uri.equals("/healthz")) {
                      res = plain(200, "ok");

                    } else if (uri.equals("/version")) {
                      res = plain(200, VERSION);

                    } else if (uri.equals("/config")) {
                      String body = """
                          port: %d
                          dataDir: %s
                          flushThreshold: %d
                          """
                          .formatted(finalPort, finalDataDir, store.getFlushThreshold());
                      res = plain(200, body);

                    } else if (uri.equals("/metrics")) {
                      String body = """
                          # TYPE warpkv_get_total counter
                          warpkv_get_total %d
                          # TYPE warpkv_put_total counter
                          warpkv_put_total %d
                          # TYPE warpkv_delete_total counter
                          warpkv_delete_total %d
                          """
                          .formatted(store.getCount.get(), store.putCount.get(), store.delCount.get());
                      res = plain(200, body);

                    } else if (uri.equals("/admin/info")) {
                      String body = """
                          dataDir: %s
                          flushThreshold: %d
                          counts: get=%d put=%d del=%d
                          at: %s
                          """
                          .formatted(finalDataDir, store.getFlushThreshold(),
                              store.getCount.get(), store.putCount.get(), store.delCount.get(),
                              Instant.now());
                      res = plain(200, body);

                    } else if (uri.startsWith("/admin/flush-threshold")) {
                      Map<String, String> q = parseQuery(uri);
                      int n = Integer.parseInt(q.getOrDefault("n", "0"));
                      if (n <= 0) {
                        res = plain(400, "bad n: must be > 0\n");
                      } else {
                        store.setFlushThreshold(n);
                        res = plain(200, "set flush-threshold to " + n + "\n");
                      }

                    } else if (uri.equals("/admin/flush")) {
                      String name = store.flushToSstable();
                      res = plain(200, "flushed: " + name + "\n");

                    } else if (uri.startsWith("/kv/put")) {
                      Map<String, String> q = parseQuery(uri);
                      String key = q.get("k");
                      if (key == null || key.isEmpty()) {
                        res = plain(400, "missing k\n");
                      } else {
                        byte[] bodyBytes = new byte[req.content().readableBytes()];
                        req.content().readBytes(bodyBytes);
                        String value = new String(bodyBytes, StandardCharsets.UTF_8);
                        store.put(key, value);
                        res = plain(200, "ok");
                        System.out.printf("metrics: get=%d put=%d del=%d%n",
                            store.getCount.get(), store.putCount.get(), store.delCount.get());
                      }

                    } else if (uri.startsWith("/kv/get")) {
                      Map<String, String> q = parseQuery(uri);
                      String key = q.get("k");
                      if (key == null || key.isEmpty()) {
                        res = plain(400, "missing k\n");
                      } else {
                        var v = store.get(key);
                        if (v.isPresent()) {
                          res = plain(200, v.get());
                        } else {
                          res = plain(404, "not found");
                        }
                        System.out.printf("metrics: get=%d put=%d del=%d%n",
                            store.getCount.get(), store.putCount.get(), store.delCount.get());
                      }

                    } else {
                      res = plain(404, "not found\n");
                    }

                  } catch (Exception e) {
                    res = plain(500, "error: " + e.getMessage() + "\n");
                  }

                  addCors(res);
                  ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
                }
              });
            }
          });

      Channel ch = bootstrap.bind(finalPort).sync().channel();
      ch.closeFuture().sync();

    } finally {
      boss.shutdownGracefully();
      worker.shutdownGracefully();
    }
  }

  // ---------------- helpers ----------------

  private static Map<String, String> parseQuery(String uri) {
    Map<String, String> out = new LinkedHashMap<>();
    int qi = uri.indexOf('?');
    if (qi < 0 || qi == uri.length() - 1) return out;
    String qs = uri.substring(qi + 1);
    for (String kv : qs.split("&")) {
      String[] parts = kv.split("=", 2);
      if (parts.length == 2) {
        out.put(
            URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
            URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
        );
      } else if (parts.length == 1 && !parts[0].isEmpty()) {
        out.put(URLDecoder.decode(parts[0], StandardCharsets.UTF_8), "");
      }
    }
    return out;
  }

  private static FullHttpResponse plain(int status, String body) {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    FullHttpResponse res = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.valueOf(status),
        io.netty.buffer.Unpooled.wrappedBuffer(bytes)
    );
    res.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8");
    res.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
    return res;
  }

  private static void addCors(FullHttpResponse res) {
    res.headers().set("Access-Control-Allow-Origin", "*");
    res.headers().set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    res.headers().set("Access-Control-Allow-Headers", "Content-Type,Accept");
  }
}

