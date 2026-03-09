package internal

import (
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// GenClaim generates an insurance claim event — PRD §4.3
// Suspicious accounts file claims very soon after policy start.
func GenClaim() *kgo.Record {
	accountID := randomAccount()
	suspicious := isSuspicious(accountID)
	eventTime := nowMillis()

	var policyStartTs int64
	if suspicious && rng.Float64() < 0.4 {
		// Anomalous: claim filed 1-5 days after policy start (< 30 days threshold)
		daysAgo := int64(1 + rng.Intn(5))
		policyStartTs = eventTime - daysAgo*24*3600*1000
	} else {
		// Normal: policy started 30-365 days ago
		daysAgo := int64(30 + rng.Intn(335))
		policyStartTs = eventTime - daysAgo*24*3600*1000
	}

	var claimAmount float64
	if suspicious && rng.Float64() < 0.3 {
		claimAmount = 10000 + rng.Float64()*90000 // high claim
	} else {
		claimAmount = 500 + rng.Float64()*9500 // normal claim
	}

	location, _ := randomLocationForAccount(accountID)

	event := map[string]interface{}{
		"event_id":          generateUUID(),
		"event_type":        "claim",
		"event_time":        eventTime,
		"account_id":        accountID,
		"claim_id":          fmt.Sprintf("CLM-%08d", rng.Intn(99999999)),
		"claim_amount":      round2(claimAmount),
		"claim_type":        randomClaimType(),
		"policy_id":         fmt.Sprintf("POL-%06d", rng.Intn(999999)),
		"policy_start_ts":   policyStartTs,
		"description":       randomClaimDescription(),
		"location":          location,
		"night_transaction": isNightTime(),
	}

	data, _ := json.Marshal(event)
	return &kgo.Record{
		Topic: "claims",
		Key:   []byte(accountID),
		Value: data,
	}
}

func randomClaimType() string {
	types := []string{"auto_collision", "auto_theft", "home_fire",
		"home_water", "health_emergency", "travel_cancellation", "liability"}
	return types[rng.Intn(len(types))]
}

func randomClaimDescription() string {
	descs := []string{
		"Vehicle collision at intersection",
		"Water damage from pipe burst",
		"Theft of personal property",
		"Fire damage to kitchen",
		"Medical emergency treatment",
		"Flight cancellation due to weather",
		"Property liability claim",
	}
	return descs[rng.Intn(len(descs))]
}
