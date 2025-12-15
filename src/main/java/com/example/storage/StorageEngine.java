package com.example.storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

/**
 * A tiny log-structured key-value store.
 *
 * Features:
 *  - Append-only segment files
 *  - In-memory index mapping key -> (segment, offset)
 *  - Simple compaction that rewrites only the latest values
 *
 * This is intentionally simplified so it is easy to read and extend
 * during interviews or personal learning.
 */
public class StorageEngine {

    private static final String SEGMENT_PREFIX = "segment-";
    private static final String SEGMENT_SUFFIX = ".log";

    private final Path dataDir;
    private final long maxSegmentSizeBytes;

    private final Map<String, IndexEntry> index = new HashMap<>();
    private SegmentFile activeSegment;
    private final List<SegmentFile> sealedSegments = new ArrayList<>();

    public StorageEngine(Path dataDir, long maxSegmentSizeBytes) throws IOException {
        this.dataDir = dataDir;
        this.maxSegmentSizeBytes = maxSegmentSizeBytes;
        Files.createDirectories(dataDir);
        loadExistingSegments();
    }

    public synchronized void put(String key, String value) throws IOException {
        ensureActiveSegment();
        byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        long offset = activeSegment.append(keyBytes, valueBytes);
        index.put(key, new IndexEntry(activeSegment.getPath(), offset));

        if (activeSegment.size() >= maxSegmentSizeBytes) {
            rollSegment();
        }
    }

    public synchronized String get(String key) throws IOException {
        IndexEntry entry = index.get(key);
        if (entry == null) return null;
        return readEntry(entry);
    }

    public synchronized void compact() throws IOException {
        if (index.isEmpty()) return;

        Path compactedPath = dataDir.resolve(SEGMENT_PREFIX + "compacted" + SEGMENT_SUFFIX);
        try (SegmentFile compacted = SegmentFile.createNew(compactedPath)) {
            Map<Path, RandomAccessFile> openFiles = new HashMap<>();
            try {
                for (Map.Entry<String, IndexEntry> e : index.entrySet()) {
                    String key = e.getKey();
                    IndexEntry idx = e.getValue();
                    String currentValue = readEntry(idx, openFiles);
                    if (currentValue == null) continue;

                    byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    byte[] valueBytes = currentValue.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    long offset = compacted.append(keyBytes, valueBytes);
                    index.put(key, new IndexEntry(compacted.getPath(), offset));
                }
            } finally {
                for (RandomAccessFile raf : openFiles.values()) {
                    raf.close();
                }
            }

            // Delete old sealed segments and the active segment
            for (SegmentFile sealed : sealedSegments) {
                Files.deleteIfExists(sealed.getPath());
            }
            if (activeSegment != null) {
                Files.deleteIfExists(activeSegment.getPath());
            }

            sealedSegments.clear();
            activeSegment = compacted;
        }
    }

    private void loadExistingSegments() throws IOException {
        List<Path> segmentPaths = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dataDir, SEGMENT_PREFIX + "*" + SEGMENT_SUFFIX)) {
            for (Path p : ds) segmentPaths.add(p);
        }

        if (segmentPaths.isEmpty()) {
            // Fresh engine
            activeSegment = SegmentFile.createNew(nextSegmentPath(0));
            return;
        }

        segmentPaths.sort(Comparator.comparing(Path::toString));
        for (int i = 0; i < segmentPaths.size(); i++) {
            Path p = segmentPaths.get(i);
            SegmentFile seg = SegmentFile.openExisting(p);
            if (i == segmentPaths.size() - 1) {
                activeSegment = seg;
            } else {
                sealedSegments.add(seg);
            }
            rebuildIndexFromSegment(seg);
        }
    }

    private void rebuildIndexFromSegment(SegmentFile seg) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(seg.getPath().toFile(), "r")) {
            long offset = 0;
            while (offset < raf.length()) {
                raf.seek(offset);
                try {
                    int keyLen = raf.readInt();
                    int valueLen = raf.readInt();
                    if (keyLen < 0 || valueLen < 0) break;

                    byte[] keyBytes = raf.readNBytes(keyLen);
                    if (keyBytes.length < keyLen) break;
                    byte[] valueBytes = raf.readNBytes(valueLen);
                    if (valueBytes.length < valueLen) break;

                    String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                    index.put(key, new IndexEntry(seg.getPath(), offset));

                    offset = raf.getFilePointer();
                } catch (EOFException eof) {
                    break;
                }
            }
        }
    }

    private void ensureActiveSegment() throws IOException {
        if (activeSegment == null) {
            activeSegment = SegmentFile.createNew(nextSegmentPath(sealedSegments.size()));
        }
    }

    private void rollSegment() throws IOException {
        sealedSegments.add(activeSegment);
        activeSegment = SegmentFile.createNew(nextSegmentPath(sealedSegments.size()));
    }

    private Path nextSegmentPath(int index) {
        return dataDir.resolve(SEGMENT_PREFIX + index + SEGMENT_SUFFIX);
    }

    private String readEntry(IndexEntry entry) throws IOException {
        return readEntry(entry, null);
    }

    private String readEntry(IndexEntry entry, Map<Path, RandomAccessFile> cache) throws IOException {
        RandomAccessFile raf;
        if (cache != null) {
            raf = cache.computeIfAbsent(entry.path(), p -> {
                try {
                    return new RandomAccessFile(p.toFile(), "r");
                } catch (FileNotFoundException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } else {
            raf = new RandomAccessFile(entry.path().toFile(), "r");
        }

        try {
            raf.seek(entry.offset());
            int keyLen = raf.readInt();
            int valueLen = raf.readInt();
            if (keyLen < 0 || valueLen < 0) return null;
            byte[] keyBytes = raf.readNBytes(keyLen);
            if (keyBytes.length < keyLen) return null;
            byte[] valueBytes = raf.readNBytes(valueLen);
            if (valueBytes.length < valueLen) return null;
            return new String(valueBytes, java.nio.charset.StandardCharsets.UTF_8);
        } finally {
            if (cache == null) {
                raf.close();
            }
        }
    }

    private record IndexEntry(Path path, long offset) {}

}
