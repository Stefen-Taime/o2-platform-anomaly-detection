package com.coinbase.o2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PRINCIPLE 2: Logical Clocks
 *
 * Custom TimestampExtractor that uses the event_time field from the event payload,
 * NOT the Kafka processing_time (log append time).
 *
 * This ensures deterministic output:
 * - Same input events → same output regardless of when they arrive on Kafka
 * - Windowed aggregations align on event_time
 * - Late events are processed correctly based on their actual occurrence time
 */
public class EventTimeExtractor implements TimestampExtractor {

    private static final Logger log = LoggerFactory.getLogger(EventTimeExtractor.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() == null) {
            return partitionTime;
        }

        try {
            String value = record.value().toString();
            JsonNode node = mapper.readTree(value);

            // Try event_time field (epoch millis)
            if (node.has("event_time")) {
                long eventTime = node.get("event_time").asLong();
                if (eventTime > 0) {
                    return eventTime;
                }
            }

            // Fallback: try timestamp field
            if (node.has("timestamp")) {
                long timestamp = node.get("timestamp").asLong();
                if (timestamp > 0) {
                    return timestamp;
                }
            }

        } catch (Exception e) {
            log.debug("Failed to extract event_time, using partition time: {}", e.getMessage());
        }

        // Final fallback: partition time
        return partitionTime;
    }
}
