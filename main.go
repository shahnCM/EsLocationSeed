package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/joho/godotenv"
)

var (
	esURL       string
	esIndex     string
	csvFile     string
	bulkSize    = 400
	trackerFile string
	imported    = 0
)

func init() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %s", err)
	}

	esURL = os.Getenv("ES_URL")
	esIndex = os.Getenv("ES_INDEX")
	csvFile = os.Getenv("CSV_FILE")
	trackerFile = getTrackerFileName(csvFile)
}

func main() {
	// Initialize Elasticsearch client
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esURL},
	})
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}

	// Ping Elasticsearch
	res, err := es.Info()
	if err != nil {
		log.Fatalf("Error pinging Elasticsearch: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Elasticsearch returned an error: %s", res.String())
	}

	// Set up signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Load last ID tracker
	lastID, err := getLastID()
	if err != nil {
		log.Fatalf("Error retrieving last processed ID: %s", err)
	}

	// Open the CSV file
	file, err := os.Open(csvFile)
	if err != nil {
		log.Fatalf("Error opening CSV file: %s", err)
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(bufio.NewReader(file))

	// Retrieve total number of records for progress bar
	// totalRecords, err := getTotalRecords(csvFile)
	// if err != nil {
	// 	log.Fatalf("Error counting records: %s", err)
	// }

	// progressBar := pb.Full.Start(totalRecords - 1)
	// progressBar.SetRefreshRate(500 * time.Millisecond)
	// defer progressBar.Finish()

	isStarted := lastID == ""

	// Read the header
	header, err := reader.Read()
	if err != nil {
		log.Fatal("Error reading header:", err)
	}
	fmt.Println("Header:", header)

	// Define regex for parsing latlng
	latlngRegex := regexp.MustCompile(`POINT \((\d+\.?\d*) (\d+\.?\d*)\)`)

	var bulkRequest bytes.Buffer

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				if bulkRequest.Len() > 0 {
					sendAndHandleBulk(es, &bulkRequest)
				}
				break
			}
			log.Fatalf("Error reading CSV file: %s", err)
		}

		if !isStarted && record[0] == lastID {
			isStarted = true
			continue
		}

		if isStarted {
			imported++
			log.Println("Imported: ", imported)
			// Parse latlng field
			latlngMatches := latlngRegex.FindStringSubmatch(record[9])
			if len(latlngMatches) != 3 {
				log.Fatalf("Error parsing latlng field: %s", record[9])
			}

			lat, _ := strconv.ParseFloat(latlngMatches[2], 64)
			lon, _ := strconv.ParseFloat(latlngMatches[1], 64)

			// Create a new Elasticsearch document
			document := map[string]interface{}{
				"placeId":               record[10],
				"address":               record[3],
				"latlng":                map[string]interface{}{"lat": lat, "lon": lon},
				"types":                 strings.Split(record[13], ";"),
				"isAutocompleteAddress": record[8] == "true",
				"country":               record[5],
				"city":                  record[4],
				"division":              record[7],
				"district":              record[6],
				"postalCode":            record[12],
				"plusCode":              record[11],
			}

			// Prepare bulk request
			action := map[string]interface{}{
				"index": map[string]interface{}{
					"_index": esIndex,
					"_id":    record[0],
				},
			}
			actionBytes, _ := json.Marshal(action)
			docBytes, _ := json.Marshal(document)
			bulkRequest.Write(actionBytes)
			bulkRequest.Write([]byte("\n"))
			bulkRequest.Write(docBytes)
			bulkRequest.Write([]byte("\n"))

			// Send bulk request when bulk size is reached
			if bulkRequest.Len() > bulkSize {
				sendAndHandleBulk(es, &bulkRequest)
				saveLastID(record[0])
			}

			// progressBar.Increment()
		}
	}

	// Send remaining requests
	if bulkRequest.Len() > 0 {
		// bulkStr := bulkRequest.String()
		sendAndHandleBulk(es, &bulkRequest)
		// saveLastID(bulkStr)
		// progressBar.Increment()
	}

	// Notify completion
	fmt.Println("Upload complete.")

	// Wait for interrupt signal
	<-sigCh
	fmt.Println("Interrupt signal received, shutting down...")
}

// Sends the bulk request and handles the response
func sendAndHandleBulk(es *elasticsearch.Client, buf *bytes.Buffer) {
	req := esapi.BulkRequest{
		Body: buf,
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error executing bulk request: %s", err)
	}
	defer res.Body.Close()

	var responseMap map[string]interface{}
	json.NewDecoder(res.Body).Decode(&responseMap)
	fmt.Printf("Bulk request response: %+v\n", responseMap)

	if res.IsError() {
		log.Fatalf("Error response from Elasticsearch: %s", res.String())
	}

	buf.Reset()
}

func getTrackerFileName(csvFileName string) string {
	parts := strings.Split(csvFileName, ".")
	if len(parts) > 1 {
		return fmt.Sprintf("%s_%s_tracker.csv", parts[0], "last_id")
	}
	return csvFileName + "_tracker.csv"
}

func getLastID() (string, error) {
	data, err := os.ReadFile(trackerFile)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func saveLastID(lastID string) error {
	file, err := os.Create(trackerFile)
	if err != nil {
		return fmt.Errorf("error creating tracker file: %w", err)
	}
	defer file.Close()
	_, err = file.WriteString(lastID)
	if err != nil {
		return fmt.Errorf("error writing to tracker file: %w", err)
	}
	return nil
}

func getTotalRecords(csvFile string) (int, error) {
	file, err := os.Open(csvFile)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	count := 0
	for {
		_, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return 0, err
		}
		count++
	}
	return count, nil
}
