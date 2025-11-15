package com.neel.warpkv.storage;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/** Tracks SSTable files in creation order (newest last). */
public final class Manifest {
    private final Path path;

    public Manifest(Path dir) { this.path = dir.resolve("MANIFEST"); }

    public void append(String filename) throws IOException {
        Files.writeString(path, filename + System.lineSeparator(),
                java.nio.charset.StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public List<String> loadAll() throws IOException {
        if (!Files.exists(path)) return List.of();
        var lines = Files.readAllLines(path, java.nio.charset.StandardCharsets.UTF_8);
        return new ArrayList<>(lines);
    }
}


