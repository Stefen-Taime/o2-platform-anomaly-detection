package com.coinbase.o2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;

/**
 * PRINCIPLE 1: Logic-on-Write
 *
 * Enriches events at ingestion time by looking up the account profile
 * from DynamoDB (online feature store).
 *
 * PRINCIPLE 3: In-Memory Processing
 *
 * Maintains minimal state per account_id in DynamoDB:
 * - tx_count_2min: transaction count in last 2 minutes
 * - tx_sum_1h: sum of transaction amounts in last 1 hour
 * - tx_sum_24h: sum of transaction amounts in last 24 hours
 * - last_device_id: last known device ID
 * - last_login_ts: last login timestamp
 * - avg_tx_amount: historical average transaction amount
 * - account_age_days: days since account creation
 */
public class DynamoDBEnricher {

    private static final Logger log = LoggerFactory.getLogger(DynamoDBEnricher.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final DynamoDbClient dynamoDb;
    private final String tableName;

    public DynamoDBEnricher(String tableName, String region) {
        this.tableName = tableName;
        this.dynamoDb = DynamoDbClient.builder()
                .region(Region.of(region))
                .build();
        log.info("DynamoDB enricher initialized — table={}, region={}", tableName, region);
    }

    /**
     * Logic-on-Write: Get account profile from DynamoDB feature store.
     * Returns profile JSON node with historical features.
     */
    public JsonNode getProfile(String accountId) {
        try {
            GetItemResponse response = dynamoDb.getItem(GetItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("account_id", AttributeValue.builder().s(accountId).build()))
                    .build());

            ObjectNode profile = mapper.createObjectNode();

            if (response.hasItem()) {
                Map<String, AttributeValue> item = response.item();
                profile.put("tx_count_2min", getNumericAttr(item, "tx_count_2min", 0));
                profile.put("tx_sum_1h", getDoubleAttr(item, "tx_sum_1h", 0.0));
                profile.put("tx_sum_24h", getDoubleAttr(item, "tx_sum_24h", 0.0));
                profile.put("last_device_id", getStringAttr(item, "last_device_id", ""));
                profile.put("last_login_ts", getNumericAttr(item, "last_login_ts", 0));
                profile.put("avg_tx_amount", getDoubleAttr(item, "avg_tx_amount", 0.0));
                profile.put("account_age_days", getNumericAttr(item, "account_age_days", 0));
                profile.put("claim_count_30d", getNumericAttr(item, "claim_count_30d", 0));
                profile.put("policy_start_ts", getNumericAttr(item, "policy_start_ts", 0));
                profile.put("known_devices", getStringAttr(item, "known_devices", "[]"));
                profile.put("usual_countries", getStringAttr(item, "usual_countries", "[\"CA\"]"));
                profile.put("known", true);
            } else {
                // New account — no historical profile
                profile.put("tx_count_2min", 0);
                profile.put("tx_sum_1h", 0.0);
                profile.put("tx_sum_24h", 0.0);
                profile.put("last_device_id", "");
                profile.put("last_login_ts", 0);
                profile.put("avg_tx_amount", 0.0);
                profile.put("account_age_days", 0);
                profile.put("claim_count_30d", 0);
                profile.put("policy_start_ts", 0);
                profile.put("known_devices", "[]");
                profile.put("usual_countries", "[\"CA\"]");
                profile.put("known", false);
            }

            return profile;
        } catch (Exception e) {
            log.warn("DynamoDB getProfile failed for {}: {}", accountId, e.getMessage());
            // Return empty profile on error — don't block the pipeline
            ObjectNode fallback = mapper.createObjectNode();
            fallback.put("known", false);
            fallback.put("error", true);
            return fallback;
        }
    }

    /**
     * Logic-on-Write: Update account profile in DynamoDB after processing.
     * Only updates the minimal state needed for future scoring.
     */
    public void updateProfile(String accountId, JsonNode enrichedEvent) {
        try {
            Map<String, AttributeValueUpdate> updates = new HashMap<>();
            String eventType = enrichedEvent.has("event_type")
                    ? enrichedEvent.get("event_type").asText()
                    : "unknown";

            switch (eventType) {
                case "transaction":
                    // Increment tx_count_2min, update tx_sum_1h, tx_sum_24h
                    // PRD §4.2 — windows: 2min / 1h / 24h
                    double amount = enrichedEvent.has("amount")
                            ? enrichedEvent.get("amount").asDouble()
                            : 0.0;
                    updates.put("tx_count_2min", attrUpdate(
                            AttributeValue.builder().n("1").build(), AttributeAction.ADD));
                    updates.put("tx_sum_1h", attrUpdate(
                            AttributeValue.builder().n(String.valueOf(amount)).build(),
                            AttributeAction.ADD));
                    updates.put("tx_sum_24h", attrUpdate(
                            AttributeValue.builder().n(String.valueOf(amount)).build(),
                            AttributeAction.ADD));
                    updates.put("last_tx_ts", attrUpdate(
                            AttributeValue.builder().n(String.valueOf(
                                    enrichedEvent.has("event_time")
                                            ? enrichedEvent.get("event_time").asLong()
                                            : System.currentTimeMillis()
                            )).build(), AttributeAction.PUT));
                    break;

                case "login":
                    String deviceId = enrichedEvent.has("device_id")
                            ? enrichedEvent.get("device_id").asText()
                            : "";
                    updates.put("last_device_id", attrUpdate(
                            AttributeValue.builder().s(deviceId).build(), AttributeAction.PUT));
                    updates.put("last_login_ts", attrUpdate(
                            AttributeValue.builder().n(String.valueOf(
                                    enrichedEvent.has("event_time")
                                            ? enrichedEvent.get("event_time").asLong()
                                            : System.currentTimeMillis()
                            )).build(), AttributeAction.PUT));
                    break;

                case "claim":
                    updates.put("claim_count_30d", attrUpdate(
                            AttributeValue.builder().n("1").build(), AttributeAction.ADD));
                    break;

                case "transfer":
                    updates.put("last_transfer_ts", attrUpdate(
                            AttributeValue.builder().n(String.valueOf(
                                    enrichedEvent.has("event_time")
                                            ? enrichedEvent.get("event_time").asLong()
                                            : System.currentTimeMillis()
                            )).build(), AttributeAction.PUT));
                    break;
            }

            if (!updates.isEmpty()) {
                dynamoDb.updateItem(UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("account_id",
                                AttributeValue.builder().s(accountId).build()))
                        .attributeUpdates(updates)
                        .build());
            }
        } catch (Exception e) {
            log.warn("DynamoDB updateProfile failed for {}: {}", accountId, e.getMessage());
            // Don't block pipeline on write failures
        }
    }

    public void close() {
        dynamoDb.close();
    }

    // --- Helper methods ---

    private long getNumericAttr(Map<String, AttributeValue> item, String key, long defaultVal) {
        AttributeValue val = item.get(key);
        if (val != null && val.n() != null) {
            return Long.parseLong(val.n());
        }
        return defaultVal;
    }

    private double getDoubleAttr(Map<String, AttributeValue> item, String key, double defaultVal) {
        AttributeValue val = item.get(key);
        if (val != null && val.n() != null) {
            return Double.parseDouble(val.n());
        }
        return defaultVal;
    }

    private String getStringAttr(Map<String, AttributeValue> item, String key, String defaultVal) {
        AttributeValue val = item.get(key);
        if (val != null && val.s() != null) {
            return val.s();
        }
        return defaultVal;
    }

    private AttributeValueUpdate attrUpdate(AttributeValue value, AttributeAction action) {
        return AttributeValueUpdate.builder()
                .value(value)
                .action(action)
                .build();
    }
}
