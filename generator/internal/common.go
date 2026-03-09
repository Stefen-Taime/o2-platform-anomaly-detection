package internal

import (
	"crypto/rand"
	"fmt"
	mathrand "math/rand"
	"time"
)

var rng = mathrand.New(mathrand.NewSource(time.Now().UnixNano()))

// Pool of 500 account IDs (mix of normal and suspicious) — PRD §4.2
var accountIDs = func() []string {
	ids := make([]string, 500)
	for i := range ids {
		ids[i] = fmt.Sprintf("ACC-%06d", i+1)
	}
	return ids
}()

// Suspicious accounts (~10% of pool) — these will generate more anomalies
var suspiciousAccounts = func() map[string]bool {
	m := make(map[string]bool)
	for i := 1; i <= 500; i += 10 {
		m[fmt.Sprintf("ACC-%06d", i)] = true
	}
	return m
}()

var deviceIDs = []string{
	"device-iphone-14", "device-pixel-8", "device-samsung-s24",
	"device-macbook-m3", "device-windows-pc", "device-ipad-pro",
	"device-unknown-001", "device-unknown-002", "device-unknown-003",
}

var currencies = []string{"CAD", "USD", "EUR"}

// Locations — PRD §4.4
var locations = []string{
	"Montreal, QC", "Toronto, ON", "Vancouver, BC", "Calgary, AB",
	"Ottawa, ON", "Quebec, QC", "Winnipeg, MB", "Halifax, NS",
	"New York, NY", "London, UK", "Lagos, NG", "Moscow, RU",
}

// Usual countries per account (first 6 = Canadian cities)
var usualCountries = []string{"CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "US", "GB", "NG", "RU"}

func randomAccount() string {
	return accountIDs[rng.Intn(len(accountIDs))]
}

func isSuspicious(accountID string) bool {
	return suspiciousAccounts[accountID]
}

func randomDevice(accountID string) string {
	if isSuspicious(accountID) && rng.Float64() < 0.4 {
		// Suspicious accounts often use unknown devices
		return deviceIDs[6+rng.Intn(3)]
	}
	return deviceIDs[rng.Intn(6)]
}

func randomLocation() string {
	return locations[rng.Intn(len(locations))]
}

func randomLocationForAccount(accountID string) (string, string) {
	if isSuspicious(accountID) && rng.Float64() < 0.3 {
		// Suspicious: foreign location
		idx := 8 + rng.Intn(4)
		return locations[idx], usualCountries[idx]
	}
	idx := rng.Intn(8) // Canadian cities
	return locations[idx], usualCountries[idx]
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}

// isNightTime returns true if current hour is between 00h and 05h — PRD §5.1
func isNightTime() bool {
	h := time.Now().Hour()
	return h >= 0 && h < 5
}

// generateUUID generates a UUID v4 string — PRD §4.4
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func round2(f float64) float64 {
	return float64(int(f*100)) / 100
}
