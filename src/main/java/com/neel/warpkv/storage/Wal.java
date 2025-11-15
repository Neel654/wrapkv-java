package com.neel.warpkv.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class Wal implements AutoCloseable {
    // Record layout: [type:1][keyLen:4][valLen:4][keyBytes][valBytes]
    private static final byte PUT = 1;
    private static final byte DEL = 2;

    private final Path walPath;
    private final FileChannel ch;

    public Wal(Path dataDir) throws IOException {
        Files.createDirectories(dataDir);
        this.walPath = dataDir.resolve("wal.log");
        this.ch = FileChannel.open(walPath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.ch.position(this.ch.size()); // append at end
    }

    public synchronized void appendPut(String key, byte[] value) throws IOException {
        byte[] kb = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ByteBuffer hdr = ByteBuffer.allocate(1 + 4 + 4).order(ByteOrder.BIG_ENDIAN);
        hdr.put(PUT).putInt(kb.length).putInt(value.length).flip();
        ch.write(hdr);
        ch.write(ByteBuffer.wrap(kb));
        ch.write(ByteBuffer.wrap(value));
        ch.force(true); // fsync so crashes don’t lose data
    }

    public synchronized void appendDelete(String key) throws IOException {
        byte[] kb = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ByteBuffer hdr = ByteBuffer.allocate(1 + 4 + 4).order(ByteOrder.BIG_ENDIAN);
        hdr.put(DEL).putInt(kb.length).putInt(0).flip();
        ch.write(hdr);
        ch.write(ByteBuffer.wrap(kb));
        ch.force(true);
    }

    public void replay(BiConsumer<String,String> applyPut, Consumer<String> applyDel) throws IOException {
        if (!Files.exists(walPath)) return;
        try (FileChannel rc = FileChannel.open(walPath, StandardOpenOption.READ)) {
            long pos;
            while (true) {
                pos = rc.position();
                ByteBuffer hdr = ByteBuffer.allocate(1 + 4 + 4).order(ByteOrder.BIG_ENDIAN);
                if (!readFully(rc, hdr)) break; // clean EOF
                hdr.flip();
                byte type = hdr.get();
                int kLen = hdr.getInt();
                int vLen = hdr.getInt();
                // sanity
                if (kLen < 0 || vLen < 0 || kLen > (1<<20) || vLen > (1<<26)) {
                    rc.truncate(pos); break; // corrupt tail → truncate & stop
                }
                ByteBuffer kb = ByteBuffer.allocate(kLen);
                if (!readFully(rc, kb)) { rc.truncate(pos); break; }
                String key = new String(kb.array(), java.nio.charset.StandardCharsets.UTF_8);

                if (type == PUT) {
                    ByteBuffer vb = ByteBuffer.allocate(vLen);
                    if (!readFully(rc, vb)) { rc.truncate(pos); break; }
                    String val = new String(vb.array(), java.nio.charset.StandardCharsets.UTF_8);
                    applyPut.accept(key, val);
                } else if (type == DEL) {
                    applyDel.accept(key);
                } else {
                    rc.truncate(pos); break;
                }
            }
        }
    }

    private static boolean readFully(FileChannel c, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int r = c.read(buf);
            if (r == -1) {
                if (buf.position() == 0) return false; // clean EOF
                throw new EOFException();
            }
        }
        return true;
    }

    @Override public void close() throws IOException { ch.close(); }
}

