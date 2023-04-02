package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Set up the Kafka writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	})

	// Open the CSV file for reading
	file, err := os.Open("pit_stops.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Parse the CSV file
	reader := csv.NewReader(bufio.NewReader(file))
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Println("Error reading record:", err)
			continue
		}

		// Convert the record to a string and send it as a Kafka message
		message := kafka.Message{
			Value: []byte(strings.Join(record, ",")),
		}
		err = writer.WriteMessages(nil, message)
		if err != nil {
			fmt.Println("Error sending message:", err)
		}
	}

	// Close the Kafka writer
	err = writer.Close()
	if err != nil {
		fmt.Println("Error closing writer:", err)
	}
}
