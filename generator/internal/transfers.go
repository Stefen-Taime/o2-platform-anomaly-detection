package internal

import (
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// GenTransfer generates an inter-account transfer event — PRD §4.3
// Suspicious accounts generate high-value transfers and burst patterns.
func GenTransfer() *kgo.Record {
	accountID := randomAccount()
	suspicious := isSuspicious(accountID)

	// Destination account (different from source)
	destAccount := randomAccount()
	for destAccount == accountID {
		destAccount = randomAccount()
	}

	var amount float64
	if suspicious && rng.Float64() < 0.35 {
		// Anomalous: high-value transfer ($20,000 - $200,000)
		amount = 20000 + rng.Float64()*180000
	} else {
		// Normal: $100 - $5,000
		amount = 100 + rng.Float64()*4900
	}

	location, country := randomLocationForAccount(accountID)

	event := map[string]interface{}{
		"event_id":          generateUUID(),
		"event_type":        "transfer",
		"event_time":        nowMillis(),
		"account_id":        accountID,
		"dest_account_id":   destAccount,
		"amount":            round2(amount),
		"currency":          currencies[rng.Intn(len(currencies))],
		"transfer_type":     randomTransferType(),
		"reference":         fmt.Sprintf("REF-%012d", rng.Int63n(999999999999)),
		"institution_code":  randomInstitution(),
		"location":          location,
		"country":           country,
		"night_transaction": isNightTime(),
	}

	data, _ := json.Marshal(event)
	return &kgo.Record{
		Topic: "transfers",
		Key:   []byte(accountID),
		Value: data,
	}
}

func randomTransferType() string {
	types := []string{"e-transfer", "wire", "eft", "internal", "international"}
	return types[rng.Intn(len(types))]
}

func randomInstitution() string {
	institutions := []string{"001", "002", "003", "004", "006", "010", "016", "039"}
	return institutions[rng.Intn(len(institutions))]
}
