package internal

import (
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// GenLogin generates a user login event — PRD §4.3
// Suspicious accounts use unknown devices.
func GenLogin() *kgo.Record {
	accountID := randomAccount()
	deviceID := randomDevice(accountID) // suspicious accounts get unknown devices
	location, country := randomLocationForAccount(accountID)

	event := map[string]interface{}{
		"event_id":          generateUUID(),
		"event_type":        "login",
		"event_time":        nowMillis(),
		"account_id":        accountID,
		"device_id":         deviceID,
		"ip_address":        randomIP(),
		"user_agent":        randomUserAgent(),
		"location":          location,
		"geo_country":       country,
		"geo_city":          location,
		"success":           rng.Float64() < 0.95, // 5% failed logins
		"night_transaction": isNightTime(),
	}

	data, _ := json.Marshal(event)
	return &kgo.Record{
		Topic: "logins",
		Key:   []byte(accountID),
		Value: data,
	}
}

func randomIP() string {
	return fmt.Sprintf("%d.%d.%d.%d",
		rng.Intn(223)+1, rng.Intn(256), rng.Intn(256), rng.Intn(254)+1)
}

func randomUserAgent() string {
	agents := []string{
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)",
		"Mozilla/5.0 (Linux; Android 14; Pixel 8)",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0)",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
		"BNC-Mobile/3.2.1 (iOS)",
		"Intact-App/5.0.0 (Android)",
	}
	return agents[rng.Intn(len(agents))]
}
