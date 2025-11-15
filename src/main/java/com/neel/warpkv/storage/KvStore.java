package com.neel.warpkv.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KvStore = String-facing API on top of:
 *  - memtable (String -> byte[])
 *  - immutable SSTables (newest -> oldest)
 *
 * - Storage stays binary (byte[]); we decode/encode at the boundary.
 * - Provides the symbols Server.java expects (flush threshold, counters, etc.).
 * - Simple auto-flush when puts since last flush reach threshold.
 */
public final class KvStore implements AutoCloseable {

    // ----- metrics (Server.java reads these directly) -----
    public final AtomicInteger getCount = new AtomicInteger();
    public final AtomicInteger putCount = new AtomicInteger();
    public final AtomicInteger delCount = new AtomicInteger();

    // ----- config / state -----
    private final Path dataDir;
    private volatile int flushThreshold = 100;     // default; Server prints this at startup
    private final ConcurrentMap<String, byte[]> memtable = new ConcurrentHashMap<>();
    private final List<SstReader> sstables = new ArrayList<>();

    // tracks how many puts since last flush (auto-flush trigger)
    private final AtomicInteger putsSinceFlush = new AtomicInteger();

    public KvStore(Path dataDir) throws IOException {
        if (dataDir == null) throw new IllegalArgumentException("dataDir == null");
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);
        loadExistingSstables();
    }

    // Server expects a no-arg close() or try-with-resources friendly
    @Override
    public void close() {
        synchronized (sstables) {
            for (SstReader r : sstables) {
                try { r.close(); } catch (IOException ignored) {}
            }
        }
    }

    // -------------------- Public API --------------------

    public void put(String key, String value) {
        if (key == null) throw new IllegalArgumentException("key == null");
        if (value == null) throw new IllegalArgumentException("value == null");
        memtable.put(key, value.getBytes(StandardCharsets.UTF_8));
        putCount.incrementAndGet();

        // basic auto-flush
        if (putsSinceFlush.incrementAndGet() >= flushThreshold) {
            try {
                flushToSstable();
            } catch (IOException e) {
                // Log and keep going; the data is still in memtable
                System.err.println("Auto-flush failed: " + e.getMessage());
            }
        }
    }

    public Optional<String> get(String key) {
        if (key == null) return Optional.empty();

        // memtable first
        byte[] inMem = memtable.get(key);
        if (inMem != null) {
            getCount.incrementAndGet();
            return Optional.of(new String(inMem, StandardCharsets.UTF_8));
        }

        // sstables newest -> oldest
        List<SstReader> snapshot;
        synchronized (sstables) {
            snapshot = new ArrayList<>(sstables);
        }
        for (SstReader sst : snapshot) {
            try {
                Optional<byte[]> vb = sst.get(key);
                if (vb.isPresent()) {
                    getCount.incrementAndGet();
                    return Optional.of(new String(vb.get(), StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                // Warn and continue; a single corrupted SST shouldn't kill reads
                System.err.println("SST read failed from " + sst + ": " + e.getMessage());
            }
        }
        getCount.incrementAndGet(); // count the miss too, keeps metrics simple
        return Optional.empty();
    }

    public void delete(String key) {
        if (key == null) return;
        // Simple delete: remove from memtable (no tombstone persisted here)
        memtable.remove(key);
        delCount.incrementAndGet();
    }

    // -------------------- Flush controls expected by Server --------------------

    public int getFlushThreshold() {
        return flushThreshold;
    }

    public void setFlushThreshold(int n) {
        if (n <= 0) throw new IllegalArgumentException("flushThreshold must be > 0");
        this.flushThreshold = n;
    }

    /**
     * Flush memtable to a new SSTable file.
     * File format matches SstReader: repeated [int keyLen][int valLen][key][value] (BIG_ENDIAN).
     * Returns the filename for logging.
     */
    public String flushToSstable() throws IOException {
        // Snapshot memtable to avoid long lock; if empty, do nothing.
        if (memtable.isEmpty()) return "(no-op)";
        List<Entry> snapshot = new ArrayList<>(memtable.size());
        for (var e : memtable.entrySet()) {
            snapshot.add(new Entry(e.getKey(), e.getValue()));
        }

        // Write new SST (timestamp-based name)
        String fname = "sst_" + System.currentTimeMillis() + ".sst";
        Path out = dataDir.resolve(fname);
        writeSst(out, snapshot);

        // Clear memtable entries we flushed
        for (Entry e : snapshot) {
            // Only remove if mapping unchanged (avoid racing with new writes)
            memtable.remove(e.key, e.value);
        }
        putsSinceFlush.set(0);

        // Load the new SST for reads (newest at front)
        SstReader reader = new SstReader(out);
        synchronized (sstables) {
            sstables.add(0, reader);
        }

        return fname;
    }

    // -------------------- Internal helpers --------------------

    private void loadExistingSstables() throws IOException {
        if (!Files.exists(dataDir)) return;

        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dataDir, "sst_*.sst")) {
            for (Path p : ds) files.add(p);
        } catch (IOException ignored) {
            // If the pattern fails, fall back to scanning all and filtering
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(dataDir)) {
                for (Path p : ds) {
                    String n = p.getFileName().toString();
                    if (n.startsWith("sst_") && n.endsWith(".sst")) files.add(p);
                }
            }
        }

        // sort newest first by filename timestamp (or last-modified as fallback)
        files.sort(new Comparator<>() {
            @Override public int compare(Path a, Path b) {
                String as = a.getFileName().toString();
                String bs = b.getFileName().toString();
                long at = parseStamp(as);
                long bt = parseStamp(bs);
                int cmp = Long.compare(bt, at);
                if (cmp != 0) return cmp;
                try {
                    return Files.getLastModifiedTime(b).compareTo(Files.getLastModifiedTime(a));
                } catch (IOException e) {
                    return 0;
                }
            }
            private long parseStamp(String name) {
                // sst_1699999999999.sst
                try {
                    int us = name.indexOf('_');
                    int dot = name.lastIndexOf('.');
                    if (us >= 0 && dot > us) {
                        return Long.parseLong(name.substring(us + 1, dot));
                    }
                } catch (Exception ignored) {}
                return 0L;
            }
        });

        synchronized (sstables) {
            for (Path p : files) {
                try {
                    sstables.add(new SstReader(p));
                } catch (IOException e) {
                    System.err.println("Skipping unreadable SST " + p + ": " + e.getMessage());
                }
            }
        }
    }

    private void writeSst(Path out, List<Entry> rows) throws IOException {
        try (FileChannel ch = FileChannel.open(out,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {

            ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
            for (Entry e : rows) {
                byte[] k = e.key.getBytes(StandardCharsets.UTF_8);
                byte[] v = e.value;

                header.clear();
                header.putInt(k.length);
                header.putInt(v.length);
                header.flip();
                ch.write(header);

                ch.write(ByteBuffer.wrap(k));
                ch.write(ByteBuffer.wrap(v));
            }
            ch.force(true);
        }
    }

    private record Entry(String key, byte[] value) {}

    @Override
    public String toString() {
        return "KvStore{dataDir=" + dataDir + ", sstables=" + sstables.size() + ", at=" + Instant.now() + "}";
    }
}

