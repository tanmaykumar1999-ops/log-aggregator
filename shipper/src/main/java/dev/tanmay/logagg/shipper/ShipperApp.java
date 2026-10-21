package dev.tanmay.logagg.shipper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.tanmay.logagg.model.LogEvent;

public class ShipperApp {

    private static final Logger log = LoggerFactory.getLogger(ShipperApp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String UNKNOWN_LEVEL = "UNKNOWN";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: ShipperApp <log-file-path> <service-name>");
            System.exit(2);
        }
        Path logFile = Path.of(args[0]);
        String service = args[1];

        String bootstrap = envOr("KAFKA_BOOTSTRAP", "localhost:9092");
        String registryUrl = envOr("SCHEMA_REGISTRY_URL", "http://localhost:8081");
        String topic = envOr("LOGS_TOPIC", "logs.raw");
        String host = resolveHost();

        log.info("Starting shipper: file={} service={} host={} bootstrap={} registry={} topic={}",
                logFile, service, host, bootstrap, registryUrl, topic);

        try (LogEventProducer producer = new LogEventProducer(bootstrap, registryUrl, topic)) {
            FileTailer tailer = new FileTailer(logFile, line -> {
                if (line.isBlank()) {
                    return;
                }
                producer.send(toEvent(line, service, host));
            });

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received");
                tailer.stop();
                producer.close();
            }, "shipper-shutdown"));

            tailer.run();
        } catch (IOException e) {
            log.error("Failed while tailing {}", logFile, e);
            System.exit(1);
        }
    }

    static LogEvent toEvent(String rawLine, String service, String host) {
        LogEvent.Builder builder = LogEvent.newBuilder()
                .setService(service)
                .setHost(host)
                .setRaw(rawLine)
                .setTimestamp(Instant.now());

        String trimmed = rawLine.trim();
        JsonNode json = trimmed.startsWith("{") ? parseJson(trimmed) : null;

        if (json == null) {
            return builder
                    .setLevel(UNKNOWN_LEVEL)
                    .setMessage(trimmed)
                    .build();
        }

        return builder
                .setLevel(json.path("level").asText(UNKNOWN_LEVEL))
                .setMessage(json.path("message").asText(trimmed))
                .setTraceId(json.path("trace_id").asText(null))
                .build();
    }

    private static JsonNode parseJson(String text) {
        try {
            return MAPPER.readTree(text);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private static String resolveHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.warn("Could not resolve local hostname, using 'unknown'", e);
            return "unknown";
        }
    }

    private static String envOr(String envVar, String defaultValue) {
        String value = System.getenv(envVar);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }
}