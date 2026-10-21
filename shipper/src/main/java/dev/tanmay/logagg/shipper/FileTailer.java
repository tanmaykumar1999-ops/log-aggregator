package dev.tanmay.logagg.shipper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTailer {

    private static final Logger log = LoggerFactory.getLogger(FileTailer.class);

    private final Path logFilePath;
    private final Consumer<String> lineConsumer;
    private final long pollIntervalMs;
    private volatile boolean running = true;

    public FileTailer(Path logFilePath, Consumer<String> lineConsumer) {
        this(logFilePath, lineConsumer, 200);
    }

    public FileTailer(Path logFilePath, Consumer<String> lineConsumer, long pollIntervalMs) {
        this.logFilePath = Objects.requireNonNull(logFilePath, "logFilePath cannot be null");
        this.lineConsumer = Objects.requireNonNull(lineConsumer, "lineConsumer cannot be null");
        if (pollIntervalMs <= 0) {
            throw new IllegalArgumentException("pollIntervalMs must be positive");
        }
        this.pollIntervalMs = pollIntervalMs;
    }

    public void run() throws IOException {
        log.info("Tailing {} (polling every {} ms)", logFilePath, pollIntervalMs);
        long count = 0;

        try (BufferedReader reader = Files.newBufferedReader(logFilePath, StandardCharsets.UTF_8)) {
            while (running) {
                String line = reader.readLine();
                if (line != null) {
                    lineConsumer.accept(line);
                    count++;
                    if (count % 1000 == 0) {
                        log.info("Shipped {} lines so far", count);
                    }
                    continue;
                }
                try {
                    Thread.sleep(pollIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("Tailer stopped after {} lines", count);
    }

    public void stop() {
        log.info("Stop requested for {}", logFilePath);
        running = false;
    }
}