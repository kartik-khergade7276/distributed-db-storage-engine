package com.example.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

/**
 * Thin wrapper over a segment file.
 *
 * Layout:
 *   [keyLen:int][valueLen:int][key bytes][value bytes]...
 */
public class SegmentFile implements AutoCloseable {

    private final Path path;
    private final RandomAccessFile raf;

    private SegmentFile(Path path, RandomAccessFile raf) {
        this.path = path;
        this.raf = raf;
    }

    public static SegmentFile createNew(Path path) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
        raf.setLength(0);
        return new SegmentFile(path, raf);
    }

    public static SegmentFile openExisting(Path path) throws FileNotFoundException {
        RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
        return new SegmentFile(path, raf);
    }

    public long append(byte[] key, byte[] value) throws IOException {
        long offset = raf.length();
        raf.seek(offset);
        raf.writeInt(key.length);
        raf.writeInt(value.length);
        raf.write(key);
        raf.write(value);
        raf.getFD().sync();
        return offset;
    }

    public long size() throws IOException {
        return raf.length();
    }

    public Path getPath() {
        return path;
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
