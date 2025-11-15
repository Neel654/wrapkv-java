package com.neel.warpkv.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/** Tiny Bloom filter: m bits, k hashes. */
public final class BloomFilter {
    private final int m;     // total bits (>= 8 and multiple of 8)
    private final int k;     // number of hash functions
    private final byte[] bits;

    public BloomFilter(int mBits, int kHashes) {
        if (mBits < 8) throw new IllegalArgumentException("mBits >= 8");
        this.m = (mBits + 7) / 8 * 8; // round to byte multiple
        this.k = Math.max(1, kHashes);
        this.bits = new byte[this.m / 8];
    }

    public static BloomFilter fromBytes(byte[] raw) {
        ByteBuffer bb = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        int m = bb.getInt();
        int k = bb.getInt();
        byte[] bits = new byte[m/8];
        bb.get(bits);
        BloomFilter bf = new BloomFilter(m, k);
        System.arraycopy(bits, 0, bf.bits, 0, bits.length);
        return bf;
    }

    public byte[] toBytes() {
        ByteBuffer bb = ByteBuffer.allocate(4 + 4 + bits.length).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(m).putInt(k).put(bits);
        return bb.array();
    }

    public void add(byte[] key) {
        long h1 = hash64(key, 0x9E3779B97F4A7C15L);
        long h2 = hash64(key, 0xC2B2AE3D27D4EB4FL);
        for (int i = 0; i < k; i++) {
            long h = h1 + i * h2;
            int idx = (int) (unsignedMod(h, m));
            setBit(idx);
        }
    }

    public boolean mightContain(byte[] key) {
        long h1 = hash64(key, 0x9E3779B97F4A7C15L);
        long h2 = hash64(key, 0xC2B2AE3D27D4EB4FL);
        for (int i = 0; i < k; i++) {
            long h = h1 + i * h2;
            int idx = (int) (unsignedMod(h, m));
            if (!getBit(idx)) return false;
        }
        return true;
    }

    private void setBit(int bit) { bits[bit >>> 3] |= (1 << (bit & 7)); }
    private boolean getBit(int bit) { return (bits[bit >>> 3] & (1 << (bit & 7))) != 0; }

    private static long unsignedMod(long x, int mod) {
        long u = x ^ (x >> 63); // make non-negative-ish
        return (u & Long.MAX_VALUE) % mod;
    }

    // fast-ish 64-bit hash
    private static long hash64(byte[] data, long seed) {
        long h = seed ^ (data.length * 0x9E3779B97F4A7C15L);
        int i = 0;
        while (i + 8 <= data.length) {
            long v = 0;
            for (int b = 0; b < 8; b++) v |= ((long)(data[i+b] & 0xff)) << (8*b);
            i += 8;
            v *= 0xC2B2AE3D27D4EB4FL;
            v = Long.rotateLeft(v, 31);
            v *= 0x9E3779B97F4A7C15L;
            h ^= v;
            h = Long.rotateLeft(h, 27) * 5 + 0x52DCE729;
        }
        long tail = 0;
        int shift = 0;
        while (i < data.length) { tail |= (long)(data[i++] & 0xff) << shift; shift += 8; }
        h ^= tail * 0x9E3779B97F4A7C15L;
        h = Long.rotateLeft(h, 33) * 0xC2B2AE3D27D4EB4FL;
        h ^= h >>> 33;
        h *= 0xFF51AFD7ED558CCDL;
        h ^= h >>> 33;
        h *= 0xC4CEB9FE1A85EC53L;
        h ^= h >>> 33;
        return h;
    }

    @Override public String toString() { return "Bloom(m=" + m + ",k=" + k + ")"; }
}

