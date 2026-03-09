package internal

import (
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// GenTransaction generates a bank transaction event — PRD §4.4
// Suspicious accounts generate high-amount / high-velocity patterns.
func GenTransaction() *kgo.Record {
	accountID := randomAccount()
	suspicious := isSuspicious(accountID)

	var amount float64
	if suspicious && rng.Float64() < 0.3 {
		// Anomalous: high amount ($5,000 - $100,000)
		amount = 5000 + rng.Float64()*95000
	} else {
		// Normal: $10 - $2,000
		amount = 10 + rng.Float64()*1990
	}

	location, country := randomLocationForAccount(accountID)

	event := map[string]interface{}{
		"event_id":          generateUUID(),
		"event_type":        "transaction",
		"event_time":        nowMillis(),
		"account_id":        accountID,
		"amount":            round2(amount),
		"currency":          currencies[rng.Intn(len(currencies))],
		"merchant":          fmt.Sprintf("Commerce %s", randomMerchant()),
		"location":          location,
		"country":           country,
		"device_id":         randomDevice(accountID),
		"channel":           randomChannel(),
		"category":          randomCategory(),
		"is_online":         rng.Float64() < 0.7,
		"night_transaction": isNightTime(),
	}

	data, _ := json.Marshal(event)
	return &kgo.Record{
		Topic: "transactions",
		Key:   []byte(accountID),
		Value: data,
	}
}

func randomCategory() string {
	cats := []string{"groceries", "dining", "travel", "electronics",
		"gas", "healthcare", "entertainment", "utilities", "transfer"}
	return cats[rng.Intn(len(cats))]
}

func randomChannel() string {
	channels := []string{"POS", "ATM", "ONLINE", "MOBILE", "WIRE"}
	return channels[rng.Intn(len(channels))]
}

func randomMerchant() string {
	merchants := []string{"IGA", "Metro", "Costco", "Jean Coutu",
		"Tim Hortons", "Amazon.ca", "Best Buy", "Shell", "Uber",
		"Apple Store", "XYZ Corp", "Transfert Interac"}
	return merchants[rng.Intn(len(merchants))]
}
