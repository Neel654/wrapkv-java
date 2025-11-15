package com.neel.warpkv.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** SST v3: Header [count:int32][indexOffset:int64][bloomOffset:int64], Footer "SST3"
 *  Index = sparse keys -> entry offsets, Bloom = all keys.
 */
public final class SstWriter {
    private static final byte[] MAGIC3 = new byte[]{'S','S','T','3'};
    private static final int INDEX_SPAN = 64;

    public static Path write(Path dir, String tableName, Map<String,String> data) throws IOException {
        Files.createDirectories(dir);
        Path p = dir.resolve(tableName);

        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE, StandardOpenOption.READ)) {

            TreeMap<String,String> sorted = (data instanceof TreeMap<?,?> t)
                    ? (TreeMap<String,String>) data
                    : new TreeMap<>(data);

            // Header placeholder
            ByteBuffer hdr = ByteBuffer.allocate(4 + 8 + 8).order(ByteOrder.BIG_ENDIAN);
            hdr.putInt(sorted.size()); // count
            hdr.putLong(0L);           // indexOffset
            hdr.putLong(0L);           // bloomOffset
            hdr.flip(); ch.write(hdr);

            // Entries + collect index + bloom
            List<String> indexKeys = new ArrayList<>();
            List<Long>   indexOffs = new ArrayList<>();
            BloomFilter bloom = new BloomFilter(Math.max(1024, sorted.size() * 10), 7);

            int i = 0;
            for (var e : sorted.entrySet()) {
                long entryStart = ch.position();
                if (i % INDEX_SPAN == 0) {
                    indexKeys.add(e.getKey());
                    indexOffs.add(entryStart);
                }
                byte[] kb = e.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                byte[] vb = e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                // bloom add
                bloom.add(kb);

                ByteBuffer meta = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                meta.putInt(kb.length).putInt(vb.length).flip();
                ch.write(meta);
                ch.write(ByteBuffer.wrap(kb));
                ch.write(ByteBuffer.wrap(vb));
                i++;
            }

            // Write index
            long indexOffset = ch.position();
            ByteBuffer idxHdr = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
            idxHdr.putInt(indexKeys.size()).flip();
            ch.write(idxHdr);
            for (int j=0;j<indexKeys.size();j++) {
                byte[] kb = indexKeys.get(j).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                ByteBuffer rec = ByteBuffer.allocate(4 + kb.length + 8).order(ByteOrder.BIG_ENDIAN);
                rec.putInt(kb.length).put(kb).putLong(indexOffs.get(j)).flip();
                ch.write(rec);
            }

            // Write bloom block
            long bloomOffset = ch.position();
            byte[] bloomBytes = bloom.toBytes();
            ch.write(ByteBuffer.wrap(bloomBytes));

            // Footer
            ch.write(ByteBuffer.wrap(MAGIC3));

            // Patch header with offsets
            ByteBuffer patch = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
            patch.putLong(indexOffset).putLong(bloomOffset).flip();
            ch.position(4); // after count
            ch.write(patch);

            ch.force(true);
        }
        return p;
    }
}

