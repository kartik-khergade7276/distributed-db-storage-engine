package com.example.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;

/**
 * Simple CLI wrapper around the StorageEngine.
 *
 * This is intentionally tiny: the goal is to demonstrate the storage engine
 * behaviour, not to build a full database server.
 */
public class Main {

    public static void main(String[] args) throws IOException {
        Path dataDir = Path.of("data");
        Files.createDirectories(dataDir);

        StorageEngine engine = new StorageEngine(dataDir, 1024 * 16); // 16 KB segments

        System.out.println("=== Distributed-style Storage Engine Demo ===");
        System.out.println("Data directory: " + dataDir.toAbsolutePath());
        System.out.println("Commands:");
        System.out.println("  PUT <key> <value>");
        System.out.println("  GET <key>");
        System.out.println("  COMPACT");
        System.out.println("  EXIT");
        System.out.println();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            if (!scanner.hasNextLine()) break;
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+", 3);
            String cmd = parts[0].toUpperCase();

            try {
                switch (cmd) {
                    case "PUT" -> {
                        if (parts.length < 3) {
                            System.out.println("Usage: PUT <key> <value>");
                            continue;
                        }
                        String key = parts[1];
                        String value = parts[2];
                        engine.put(key, value);
                        System.out.println("OK");
                    }
                    case "GET" -> {
                        if (parts.length < 2) {
                            System.out.println("Usage: GET <key>");
                            continue;
                        }
                        String key = parts[1];
                        String value = engine.get(key);
                        System.out.println(value == null ? "(null)" : value);
                    }
                    case "COMPACT" -> {
                        engine.compact();
                        System.out.println("Compaction completed.");
                    }
                    case "EXIT", "QUIT" -> {
                        System.out.println("Shutting down.");
                        return;
                    }
                    default -> System.out.println("Unknown command: " + cmd);
                }
            } catch (IOException e) {
                System.err.println("I/O error: " + e.getMessage());
            }
        }
    }
}
