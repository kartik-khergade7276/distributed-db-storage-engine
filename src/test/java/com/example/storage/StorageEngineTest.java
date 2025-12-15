package com.example.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic tests for the StorageEngine.
 */
public class StorageEngineTest {

    private Path tmpDir;

    private StorageEngine newEngine() throws IOException {
        tmpDir = Files.createTempDirectory("storage-engine-test");
        return new StorageEngine(tmpDir, 1024 * 4); // 4 KB segments for tests
    }

    @AfterEach
    void cleanup() throws IOException {
        if (tmpDir != null && Files.exists(tmpDir)) {
            Files.walk(tmpDir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {
                        }
                    });
        }
    }

    @Test
    void putAndGetSingleKey() throws IOException {
        StorageEngine engine = newEngine();
        engine.put("user:1", "Alice");
        assertEquals("Alice", engine.get("user:1"));
    }

    @Test
    void overwriteKeyAndReadLatest() throws IOException {
        StorageEngine engine = newEngine();
        engine.put("counter", "1");
        engine.put("counter", "2");
        engine.put("counter", "3");
        assertEquals("3", engine.get("counter"));
    }

    @Test
    void survivesCompaction() throws IOException {
        StorageEngine engine = newEngine();
        engine.put("a", "one");
        engine.put("b", "two");
        engine.put("a", "three"); // overwritten

        engine.compact();

        assertEquals("three", engine.get("a"));
        assertEquals("two", engine.get("b"));
    }

    @Test
    void nonExistingKeyReturnsNull() throws IOException {
        StorageEngine engine = newEngine();
        assertNull(engine.get("missing"));
    }
}
