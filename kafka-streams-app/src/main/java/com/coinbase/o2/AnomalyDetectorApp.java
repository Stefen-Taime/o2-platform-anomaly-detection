package com.coinbase.o2;

import com.coinbase.o2.model.Anomaly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * O2 Anomaly Detector — Kafka Streams Application
 *
 * Implements the 3 Coinbase O2 principles:
 * 1. Logic-on-Write: Enrichment at ingestion time via DynamoDB lookup
 * 2. Logical Clocks: Processing by event_time (not processing_time)
 * 3. In-Memory Processing: Minimal state per account_id in RocksDB
 */
public class AnomalyDetectorApp {

    private static final Logger log = LoggerFactory.getLogger(AnomalyDetectorApp.class);
    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public static void main(String[] args) throws IOException {
        // Configuration from environment
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String applicationId = env("APPLICATION_ID", "o2-anomaly-detector");
        String inputTopicsStr = env("INPUT_TOPICS", "transactions,claims,logins,transfers");
        String outputTopic = env("OUTPUT_TOPIC", "anomalies");
        String dynamoTable = env("DYNAMODB_TABLE", "o2-platform-feature-store");
        String dynamoRegion = env("DYNAMODB_REGION", "us-east-1");

        List<String> inputTopics = Arrays.asList(inputTopicsStr.split(","));

        // Kafka Streams properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // Logical Clocks — use event_time timestamp extractor
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                EventTimeExtractor.class.getName());

        // In-Memory Processing — optimize RocksDB for small state
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024); // 10MB

        // Build topology
        DynamoDBEnricher enricher = new DynamoDBEnricher(dynamoTable, dynamoRegion);
        AnomalyScorer scorer = new AnomalyScorer();

        Topology topology = buildTopology(inputTopics, outputTopic, enricher, scorer);
        log.info("Topology:\n{}", topology.describe());

        // Start streams
        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Kafka Streams...");
            streams.close(Duration.ofSeconds(10));
            enricher.close();
            latch.countDown();
        }));

        // Health check server (port 8080)
        startHealthServer(streams);

        try {
            streams.start();
            log.info("O2 Anomaly Detector started — consuming from {}", inputTopics);
            latch.await();
        } catch (Exception e) {
            log.error("Fatal error", e);
            System.exit(1);
        }
    }

    /**
     * Build the Kafka Streams topology implementing all 3 Coinbase principles.
     */
    static Topology buildTopology(List<String> inputTopics, String outputTopic,
                                  DynamoDBEnricher enricher, AnomalyScorer scorer) {
        StreamsBuilder builder = new StreamsBuilder();

        // Consume from all input topics
        KStream<String, String> events = builder.stream(inputTopics,
                Consumed.with(Serdes.String(), Serdes.String()));

        // ============================================================
        // PRINCIPLE 1: Logic-on-Write
        // Enrich immediately at ingestion — lookup DynamoDB profile
        // ============================================================
        KStream<String, String> enriched = events.mapValues((readOnlyKey, value) -> {
            try {
                JsonNode event = mapper.readTree(value);
                String accountId = event.has("account_id")
                        ? event.get("account_id").asText()
                        : "unknown";

                // Logic-on-Write: fetch profile from DynamoDB feature store
                JsonNode profile = enricher.getProfile(accountId);

                // Merge event + profile into enriched event
                ObjectNode enrichedEvent = mapper.createObjectNode();
                enrichedEvent.setAll((ObjectNode) event);
                enrichedEvent.set("profile", profile);

                // Determine event type from topic metadata (stored in event)
                String eventType = event.has("event_type")
                        ? event.get("event_type").asText()
                        : "unknown";
                enrichedEvent.put("event_type", eventType);

                return mapper.writeValueAsString(enrichedEvent);
            } catch (Exception e) {
                log.warn("Failed to enrich event: {}", e.getMessage());
                return value; // pass through on error
            }
        });

        // ============================================================
        // PRINCIPLE 2: Logical Clocks (handled by EventTimeExtractor)
        // All windowed operations use event_time, not processing_time
        // ============================================================

        // ============================================================
        // PRINCIPLE 3: In-Memory Processing
        // Score anomalies using minimal in-memory state
        // ============================================================
        KStream<String, String> anomalies = enriched.flatMapValues((readOnlyKey, value) -> {
            try {
                JsonNode enrichedEvent = mapper.readTree(value);
                String eventType = enrichedEvent.has("event_type")
                        ? enrichedEvent.get("event_type").asText()
                        : "unknown";

                // Score the enriched event
                Anomaly anomaly = scorer.score(enrichedEvent, eventType);

                if (anomaly != null && anomaly.getRiskScore() > 0.5) {
                    // Update DynamoDB profile with new state (Logic-on-Write update)
                    String accountId = enrichedEvent.has("account_id")
                            ? enrichedEvent.get("account_id").asText()
                            : "unknown";
                    enricher.updateProfile(accountId, enrichedEvent);

                    String anomalyJson = mapper.writeValueAsString(anomaly);
                    log.info("ANOMALY DETECTED [{}] account={} score={} rule={}",
                            eventType, accountId, anomaly.getRiskScore(), anomaly.getRule());
                    return List.of(anomalyJson);
                }

                // Even if not anomalous, update profile state
                if (enrichedEvent.has("account_id")) {
                    enricher.updateProfile(
                            enrichedEvent.get("account_id").asText(),
                            enrichedEvent);
                }

                return List.of(); // no anomaly
            } catch (Exception e) {
                log.warn("Failed to score event: {}", e.getMessage());
                return List.of();
            }
        });

        // Produce anomalies to output topic
        anomalies.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static void startHealthServer(KafkaStreams streams) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/health", exchange -> {
            KafkaStreams.State state = streams.state();
            int code = state.isRunningOrRebalancing() ? 200 : 503;
            String body = String.format("{\"status\":\"%s\",\"state\":\"%s\"}",
                    code == 200 ? "UP" : "DOWN", state);
            exchange.sendResponseHeaders(code, body.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body.getBytes());
            }
        });
        server.setExecutor(null);
        server.start();
        log.info("Health check server started on :8080");
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
}
