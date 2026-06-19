// cmd/datagen/main.go generates large realistic datasets for pipeline testing.
//
// Usage:
//
//	go run ./cmd/datagen -rows 10000 -format csv -output testdata/large/transactions_10k.csv
//	go run ./cmd/datagen -rows 10000 -format json -output testdata/large/transactions_10k.json
//	go run ./cmd/datagen -rows 100000 -format csv -output testdata/large/transactions_100k.csv -invalid-pct 5
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/brianvoe/gofakeit/v7"
)

var categories = []string{"electronics", "clothing", "food", "furniture", "automotive", "sports", "books", "toys", "health", "garden"}

type Record struct {
	Name     string  `json:"name"`
	Email    string  `json:"email"`
	Amount   float64 `json:"amount"`
	Date     string  `json:"date"`
	Category string  `json:"category"`
	Active   bool    `json:"active"`
	Quantity int     `json:"quantity"`
	Rating   float64 `json:"rating"`
}

func main() {
	rows := flag.Int("rows", 1000, "Number of rows to generate")
	format := flag.String("format", "csv", "Output format: csv or json")
	output := flag.String("output", "", "Output file path (defaults to stdout)")
	invalidPct := flag.Float64("invalid-pct", 5.0, "Percentage of intentionally invalid records (0-100)")
	seed := flag.Int64("seed", 0, "Random seed (0 = use current time)")
	flag.Parse()

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	faker := gofakeit.New(uint64(*seed))
	rng := rand.New(rand.NewSource(*seed))

	// Determine output writer
	var w *os.File
	var err error
	if *output == "" {
		w = os.Stdout
	} else {
		// Create directory if it doesn't exist
		dir := filepath.Dir(*output)
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating directory %s: %v\n", dir, err)
			os.Exit(1)
		}
		w, err = os.Create(*output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating file %s: %v\n", *output, err)
			os.Exit(1)
		}
		defer w.Close()
	}

	switch *format {
	case "csv":
		generateCSV(w, *rows, *invalidPct, faker, rng)
	case "json":
		generateJSON(w, *rows, *invalidPct, faker, rng)
	default:
		fmt.Fprintf(os.Stderr, "Unknown format: %s (use csv or json)\n", *format)
		os.Exit(1)
	}

	if *output != "" {
		fmt.Fprintf(os.Stderr, "Generated %d records (%s) → %s\n", *rows, *format, *output)
	}
}

func generateCSV(w *os.File, rows int, invalidPct float64, faker *gofakeit.Faker, rng *rand.Rand) {
	writer := csv.NewWriter(w)
	defer writer.Flush()

	// Header
	writer.Write([]string{"name", "email", "amount", "date", "category", "active", "quantity", "rating"})

	for i := 0; i < rows; i++ {
		rec := generateRecord(faker, rng)
		isInvalid := rng.Float64()*100 < invalidPct

		if isInvalid {
			rec = corruptRecord(rec, rng)
		}

		writer.Write([]string{
			rec.Name,
			rec.Email,
			fmt.Sprintf("%.2f", rec.Amount),
			rec.Date,
			rec.Category,
			fmt.Sprintf("%t", rec.Active),
			fmt.Sprintf("%d", rec.Quantity),
			fmt.Sprintf("%.1f", rec.Rating),
		})
	}
}

func generateJSON(w *os.File, rows int, invalidPct float64, faker *gofakeit.Faker, rng *rand.Rand) {
	records := make([]interface{}, 0, rows)

	for i := 0; i < rows; i++ {
		rec := generateRecord(faker, rng)
		isInvalid := rng.Float64()*100 < invalidPct

		if isInvalid {
			// For JSON, we can produce more varied corruption
			corrupted := corruptRecordJSON(rec, rng)
			records = append(records, corrupted)
		} else {
			records = append(records, rec)
		}
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "")
	encoder.Encode(records)
}

func generateRecord(faker *gofakeit.Faker, rng *rand.Rand) Record {
	// Generate realistic date within last 2 years
	baseDate := time.Now().AddDate(-2, 0, 0)
	daysOffset := rng.Intn(730)
	date := baseDate.AddDate(0, 0, daysOffset)

	return Record{
		Name:     faker.Name(),
		Email:    faker.Email(),
		Amount:   float64(rng.Intn(100000)) / 100.0, // 0.00 - 999.99
		Date:     date.Format(time.RFC3339),
		Category: categories[rng.Intn(len(categories))],
		Active:   rng.Float64() > 0.2, // 80% active
		Quantity: rng.Intn(100) + 1,   // 1-100
		Rating:   float64(rng.Intn(50)+1) / 10.0, // 0.1-5.0
	}
}

func corruptRecord(rec Record, rng *rand.Rand) Record {
	// Corrupt in various ways to exercise validation
	switch rng.Intn(5) {
	case 0: // Missing/empty name
		rec.Name = ""
	case 1: // Invalid email
		rec.Email = "not-an-email"
	case 2: // Negative amount
		rec.Amount = -float64(rng.Intn(1000))
	case 3: // Invalid date
		rec.Date = "not-a-date"
	case 4: // Empty category
		rec.Category = ""
	}
	return rec
}

func corruptRecordJSON(rec Record, rng *rand.Rand) map[string]interface{} {
	// Convert to map and corrupt
	m := map[string]interface{}{
		"name":     rec.Name,
		"email":    rec.Email,
		"amount":   rec.Amount,
		"date":     rec.Date,
		"category": rec.Category,
		"active":   rec.Active,
		"quantity": rec.Quantity,
		"rating":   rec.Rating,
	}

	switch rng.Intn(6) {
	case 0: // Missing name entirely
		delete(m, "name")
	case 1: // Invalid email
		m["email"] = "not-an-email"
	case 2: // Amount as string
		m["amount"] = "not_a_number"
	case 3: // Invalid date
		m["date"] = "yesterday"
	case 4: // Null category
		m["category"] = nil
	case 5: // Extremely large amount (out of range)
		m["amount"] = 99999999.99
	}
	return m
}
