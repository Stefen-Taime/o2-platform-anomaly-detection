package com.coinbase.o2.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Anomaly detection result — produced to the 'anomalies' topic.
 * Output schema per PRD §5.2
 */
public class Anomaly {

    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("rule")
    private String rule;

    @JsonProperty("anomaly_score")
    private double riskScore;

    @JsonProperty("risk_level")
    private String riskLevel;

    @JsonProperty("recommended_action")
    private String recommendedAction;

    @JsonProperty("description")
    private String description;

    @JsonProperty("model_version")
    private String modelVersion;

    @JsonProperty("event_time")
    private long eventTime;

    @JsonProperty("detected_at")
    private long detectedAt;

    @JsonProperty("metadata")
    private String metadata;

    // Default constructor for Jackson
    public Anomaly() {}

    private Anomaly(Builder builder) {
        this.accountId = builder.accountId;
        this.eventType = builder.eventType;
        this.rule = builder.rule;
        this.riskScore = builder.riskScore;
        this.riskLevel = builder.riskLevel;
        this.recommendedAction = builder.recommendedAction;
        this.description = builder.description;
        this.modelVersion = builder.modelVersion;
        this.eventTime = builder.eventTime;
        this.detectedAt = builder.detectedAt;
        this.metadata = builder.metadata;
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public String getAccountId() { return accountId; }
    public String getEventType() { return eventType; }
    public String getRule() { return rule; }
    public double getRiskScore() { return riskScore; }
    public String getRiskLevel() { return riskLevel; }
    public String getRecommendedAction() { return recommendedAction; }
    public String getDescription() { return description; }
    public String getModelVersion() { return modelVersion; }
    public long getEventTime() { return eventTime; }
    public long getDetectedAt() { return detectedAt; }
    public String getMetadata() { return metadata; }

    public static class Builder {
        private String accountId;
        private String eventType;
        private String rule;
        private double riskScore;
        private String riskLevel;
        private String recommendedAction;
        private String description;
        private String modelVersion = "rules-v1";
        private long eventTime;
        private long detectedAt;
        private String metadata;

        public Builder accountId(String accountId) { this.accountId = accountId; return this; }
        public Builder eventType(String eventType) { this.eventType = eventType; return this; }
        public Builder rule(String rule) { this.rule = rule; return this; }
        public Builder riskScore(double riskScore) { this.riskScore = riskScore; return this; }
        public Builder riskLevel(String riskLevel) { this.riskLevel = riskLevel; return this; }
        public Builder recommendedAction(String recommendedAction) { this.recommendedAction = recommendedAction; return this; }
        public Builder description(String description) { this.description = description; return this; }
        public Builder modelVersion(String modelVersion) { this.modelVersion = modelVersion; return this; }
        public Builder eventTime(long eventTime) { this.eventTime = eventTime; return this; }
        public Builder detectedAt(long detectedAt) { this.detectedAt = detectedAt; return this; }
        public Builder metadata(String metadata) { this.metadata = metadata; return this; }

        public Anomaly build() {
            // Auto-derive recommended_action from risk_level if not set — PRD §5.2
            if (recommendedAction == null && riskLevel != null) {
                switch (riskLevel) {
                    case "CRITICAL": recommendedAction = "BLOCK"; break;
                    case "HIGH":     recommendedAction = "REVIEW"; break;
                    case "MEDIUM":   recommendedAction = "MONITOR"; break;
                    default:         recommendedAction = "ALLOW"; break;
                }
            }
            return new Anomaly(this);
        }
    }
}
