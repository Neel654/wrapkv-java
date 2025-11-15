package com.neel.warpkv.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;

/**
 * Minimal SSTable reader.
 * File layout (repeated):
 *   [int keyLen][int valLen][key bytes][value bytes]
 * All ints are BIG_ENDIAN.
 *
 * This reader scans linearly from dataStart. You can later swap the scan
 * with a sparse index / fence pointers + Bloom check without touching callers.
 */
public final class SstReader implements AutoCloseable {
    private static final int MAX_KEY_LEN = 1 << 20;      // 1 MiB
    private static final int MAX_VAL_LEN = 16 << 20;     // 16 MiB
    private static final int HEADER_SIZE = 8;            // [int keyLen][int valLen], BIG_ENDIAN

    // If your writer sometimes prepends a small header, probe a few starts.
    private static final List<Long> CANDIDATE_DATA_STARTS = List.of(0L, 4L, 12L, 20L);

    private final Path path;
    private final FileChannel ch;
    private final long dataStart;

    public SstReader(Path path) throws IOException {
        this.path = path;
        this.ch = FileChannel.open(path, StandardOpenOption.READ);
        this.dataStart = findDataStart();
    }

    /** Convenience overload — accepts String key and returns Optional<byte[]> */
    public Optional<byte[]> get(String key) throws IOException {
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    /** Core API — byte[] key in, Optional<byte[]> value out. */
    public Optional<byte[]> get(byte[] key) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        long pos = dataStart;

        while (true) {
            header.clear();
            int read = ch.read(header, pos);
            if (read == -1) {
                // EOF — not found
                return Optional.empty();
            }
            if (read < HEADER_SIZE) {
                throw new EOFException("Truncated header at pos=" + pos + " in " + path);
            }
            header.flip();

            int kLen = header.getInt();
            int vLen = header.getInt();

            if (kLen < 0 || kLen > MAX_KEY_LEN || vLen < 0 || vLen > MAX_VAL_LEN) {
                throw new IOException("Corrupt record lengths at pos=" + pos + " in " + path +
                        " kLen=" + kLen + " vLen=" + vLen);
            }

            long keyPos = pos + HEADER_SIZE;
            long valPos = keyPos + kLen;
            long nextPos = valPos + vLen;

            // Read key
            ByteBuffer kbuf = ByteBuffer.allocate(kLen);
            int r1 = ch.read(kbuf, keyPos);
            if (r1 != kLen) {
                throw new EOFException("Truncated key at pos=" + keyPos + " in " + path);
            }
            byte[] kbytes = kbuf.array();

            if (byteArrayEquals(kbytes, key)) {
                // Read value
                ByteBuffer vbuf = ByteBuffer.allocate(vLen);
                int r2 = ch.read(vbuf, valPos);
                if (r2 != vLen) {
                    throw new EOFException("Truncated value at pos=" + valPos + " in " + path);
                }
                return Optional.of(vbuf.array());
            }

            // advance cursor
            pos = nextPos;
        }
    }

    private long findDataStart() throws IOException {
        // If your SstWriter writes a magic/version header, detect it here.
        // For now we assume data starts at 0. Kept flexible for future-proofing.
        for (long cand : CANDIDATE_DATA_STARTS) {
            if (cand == 0L) return 0L;
            // You could peek and validate a magic/version if you add one later.
        }
        return 0L;
    }

    private static boolean byteArrayEquals(byte[] a, byte[] b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }

    @Override
    public String toString() {
        return "SstReader{"+  path.getFileName() + "}";
}}

