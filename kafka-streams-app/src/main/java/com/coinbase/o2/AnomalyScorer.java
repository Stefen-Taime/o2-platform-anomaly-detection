package com.coinbase.o2;

import com.coinbase.o2.model.Anomaly;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Rule-based anomaly scorer implementing the 4 detection use cases:
 *
 * 1. TRANSACTIONS: Abnormal velocity, amount out of profile
 * 2. CLAIMS: Abnormal delay between subscription and claim
 * 3. LOGINS: Unknown device + large transfer
 * 4. TRANSFERS: Suspicious inter-account patterns
 *
 * In production, this would call the XGBoost model via Databricks serving.
 * For the POC, we use rule-based scoring that the ML model will learn to replicate.
 */
public class AnomalyScorer {

    private static final Logger log = LoggerFactory.getLogger(AnomalyScorer.class);

    // Thresholds
    private static final int TX_VELOCITY_THRESHOLD = 5;       // >5 tx in 2 min
    private static final double TX_AMOUNT_MULTIPLIER = 3.0;   // 3x average
    private static final double TX_HIGH_AMOUNT = 10000.0;     // absolute threshold
    private static final long CLAIM_SUSPICIOUS_DELAY_MS = 7L * 24 * 3600 * 1000; // 7 days
    private static final double TRANSFER_HIGH_AMOUNT = 50000.0;
    private static final int TRANSFER_BURST_THRESHOLD = 3;    // >3 transfers in burst

    /**
     * Score an enriched event for anomalies.
     * Returns null if no anomaly detected.
     */
    public Anomaly score(JsonNode enrichedEvent, String eventType) {
        JsonNode profile = enrichedEvent.get("profile");
        if (profile == null) {
            return null;
        }

        switch (eventType) {
            case "transaction":
                return scoreTransaction(enrichedEvent, profile);
            case "claim":
                return scoreClaim(enrichedEvent, profile);
            case "login":
                return scoreLogin(enrichedEvent, profile);
            case "transfer":
                return scoreTransfer(enrichedEvent, profile);
            default:
                return null;
        }
    }

    /**
     * TRANSACTION anomalies:
     * - Velocity: >5 transactions in 2 minutes
     * - Amount: >3x historical average OR >$10,000
     * - New account with high amount
     */
    private Anomaly scoreTransaction(JsonNode event, JsonNode profile) {
        String accountId = event.get("account_id").asText();
        double amount = event.has("amount") ? event.get("amount").asDouble() : 0;
        long eventTime = event.has("event_time") ? event.get("event_time").asLong() : 0;
        int txCount2min = profile.get("tx_count_2min").asInt();
        double avgAmount = profile.get("avg_tx_amount").asDouble();
        boolean isKnown = profile.get("known").asBoolean();
        int accountAgeDays = profile.get("account_age_days").asInt();

        // Rule 1: Velocity anomaly
        if (txCount2min > TX_VELOCITY_THRESHOLD) {
            double riskScore = Math.min(0.5 + (txCount2min - TX_VELOCITY_THRESHOLD) * 0.1, 1.0);
            return Anomaly.builder()
                    .accountId(accountId)
                    .eventType("transaction")
                    .rule("HIGH_VELOCITY")
                    .riskScore(riskScore)
                    .riskLevel(riskScore > 0.8 ? "CRITICAL" : "HIGH")
                    .description(String.format("%d transactions in 2 min (threshold: %d)",
                            txCount2min, TX_VELOCITY_THRESHOLD))
                    .eventTime(eventTime)
                    .detectedAt(Instant.now().toEpochMilli())
                    .metadata(String.format("{\"tx_count\":%d,\"amount\":%.2f}", txCount2min, amount))
                    .build();
        }

        // Rule 2: Amount out of profile
        if (isKnown && avgAmount > 0 && amount > avgAmount * TX_AMOUNT_MULTIPLIER) {
            double riskScore = Math.min(0.6 + (amount / avgAmount - TX_AMOUNT_MULTIPLIER) * 0.05, 1.0);
            return Anomaly.builder()
                    .accountId(accountId)
                    .eventType("transaction")
                    .rule("AMOUNT_OUT_OF_PROFILE")
                    .riskScore(riskScore)
                    .riskLevel(riskScore > 0.8 ? "CRITICAL" : "HIGH")
                    .description(String.format("Amount $%.2f is %.1fx avg ($%.2f)",
                            amount, amount / avgAmount, avgAmount))
                    .eventTime(eventTime)
                    .detectedAt(Instant.now().toEpochMilli())
                    .metadata(String.format("{\"amount\":%.2f,\"avg\":%.2f}", amount, avgAmount))
                    .build();
        }

        // Rule 3: New account + high amount
        if (!isKnown && amount > TX_HIGH_AMOUNT) {
            return Anomaly.builder()
                    .accountId(accountId)
                    .eventType("transaction")
                    .rule("NEW_ACCOUNT_HIGH_AMOUNT")
                    .riskScore(0.85)
                    .riskLevel("CRITICAL")
                    .description(String.format("New account, first tx $%.2f (threshold: $%.0f)",
                            amount, TX_HIGH_AMOUNT))
                    .eventTime(eventTime)
                    .detectedAt(Instant.now().toEpochMilli())
                    .metadata(String.format("{\"amount\":%.2f,\"account_age_days\":%d}",
                            amount, accountAgeDays))
                    .build();
        }

        return null;
    }

    /**
     * CLAIM anomalies:
     * - Suspicious delay: claim filed <7 days after policy start
     * - Multiple claims in 30 days
     */
    private Anomaly scoreClaim(JsonNode event, JsonNode profile) {
        String accountId = event.get("account_id").asText();
        long eventTime = event.has("event_time") ? event.get("event_time").asLong() : 0;
        long policyStartTs = profile.get("policy_start_ts").asLong();
        int claimCount30d = profile.get("claim_count_30d").asInt();

        // Rule 1: Claim too soon after policy start
        if (policyStartTs > 0 && eventTime - policyStartTs < CLAIM_SUSPICIOUS_DELAY_MS) {
            long daysSincePolicy = (eventTime - policyStartTs) / (24L * 3600 * 1000);
            return Anomaly.builder()
                    .accountId(accountId)
                    .eventType("claim")
                    .rule("EARLY_CLAIM")
                    .riskScore(0.75)
                    .riskLevel("HIGH")
                    .description(String.format("Claim filed %d days after policy start (threshold: 7)",
                            daysSincePolicy))
                    .eventTime(eventTime)
                    .detectedAt(Instant.now().toEpochMilli())
                    .metadata(String.format("{\"days_since_policy\":%d,\"claim_count_30d\":%d}",
                            daysSincePolicy, claimCount30d))
                    .build();
        }

        // Rule 2: Multiple claims
        if (claimCount30d >= 3) {
            return Anomaly.builder()
                    .accountId(accountId)
                    .eventType("claim")
                    .rule("MULTIPLE_CLAIMS")
                    .riskScore(0.70)
                    .riskLevel("HIGH")
                    .description(String.format("%d claims in 30 days", claimCount30d))
                    .eventTime(eventTime)
                    .detectedAt(Instant.now().toEpochMilli())
                    .metadata(String.format("{\"claim_count_30d\":%d}", claimCount30d))
                    .build();
        }

        return null;
    }

    /**
     * LOGIN anomalies:
     * - Unknown device + large transfer after login
     */
    private Anomaly scoreLogin(JsonNode event, JsonNode profile) {
        String accountId = event.get("account_id").asText();
        long eventTime = event.has("event_time") ? event.get("event_time").asLong() : 0;
        String deviceId = event.has("device_id") ? event.get("device_id").asText() : "";
        String lastDeviceId = profile.get("last_device_id").asText();
        boolean isKnown = profile.get("known").asBoolean();

        // Rule: New device on known account
        if (isKnown && !lastDeviceId.isEmpty() && !deviceId.equals(lastDeviceId)) {
            return Anomaly.builder()
                    .accountId(accountId)
                    .eventType("login")
                    .rule("UNKNOWN_DEVICE")
                    .riskScore(0.65)
                    .riskLevel("MEDIUM")
                    .description(String.format("Login from unknown device %s (expected: %s)",
                            deviceId, lastDeviceId))
                    .eventTime(eventTime)
                    .detectedAt(Instant.now().toEpochMilli())
                    .metadata(String.format("{\"device_id\":\"%s\",\"expected_device\":\"%s\"}",
                            deviceId, lastDeviceId))
                    .build();
        }

        return null;
    }

    /**
     * TRANSFER anomalies:
     * - High-value transfer
     * - Burst of transfers (suspected layering)
     */
    private Anomaly scoreTransfer(JsonNode event, JsonNode profile) {
        String accountId = event.get("account_id").asText();
        long eventTime = event.has("event_time") ? event.get("event_time").asLong() : 0;
        double amount = event.has("amount") ? event.get("amount").asDouble() : 0;
        int txCount2min = profile.get("tx_count_2min").asInt();

        // Rule 1: High-value transfer
        if (amount > TRANSFER_HIGH_AMOUNT) {
            return Anomaly.builder()
                    .accountId(accountId)
                    .eventType("transfer")
                    .rule("HIGH_VALUE_TRANSFER")
                    .riskScore(0.80)
                    .riskLevel("HIGH")
                    .description(String.format("Transfer $%.2f exceeds threshold $%.0f",
                            amount, TRANSFER_HIGH_AMOUNT))
                    .eventTime(eventTime)
                    .detectedAt(Instant.now().toEpochMilli())
                    .metadata(String.format("{\"amount\":%.2f}", amount))
                    .build();
        }

        // Rule 2: Burst of transfers (layering pattern)
        if (txCount2min > TRANSFER_BURST_THRESHOLD) {
            return Anomaly.builder()
                    .accountId(accountId)
                    .eventType("transfer")
                    .rule("TRANSFER_BURST")
                    .riskScore(0.70)
                    .riskLevel("HIGH")
                    .description(String.format("%d transfers in 2 min (suspected layering)",
                            txCount2min))
                    .eventTime(eventTime)
                    .detectedAt(Instant.now().toEpochMilli())
                    .metadata(String.format("{\"transfer_count\":%d,\"amount\":%.2f}",
                            txCount2min, amount))
                    .build();
        }

        return null;
    }
}
