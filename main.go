package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/joho/godotenv"
)

type DataSourceInfo struct {
	DataStoreId int             `json:"dataStoreId"`
	ResourceId  int             `json:"resourceId"`
	DataType    string          `json:"dataType"`
	Payload     json.RawMessage `json:"payload"`
}

func main() {
	err := godotenv.Load()

	if err != nil {
		fmt.Println(err)
		log.Fatal("Error loading .env file")
	}

	kafkaBootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"group.id":          "bmykhaylivvv-group",
		"auto.offset.reset": "earliest",
	}
	consumer, err := kafka.NewConsumer(kafkaConfig)

	if err != nil {
		panic(err)
	}

	topic := "data-source-info-topic"
	err = consumer.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		panic(err)
	}

	influxDbConnectionUrl := os.Getenv("INFLUX_DB_URL")
	influxDbApiToken := os.Getenv("INFLUX_DB_TOKEN")
	influxDbOrg := os.Getenv("INFLUX_DB_ORG")

	influxDBClient := influxdb2.NewClient(influxDbConnectionUrl, influxDbApiToken)
	recordCountWriteApi := influxDBClient.WriteAPIBlocking(influxDbOrg, "bmykhaylivvv-bucket")

	fmt.Println("Consuming messages from topic:", topic)

	for {
		msg, err := consumer.ReadMessage(-1)

		if err == nil {
			var dataSourceInfo DataSourceInfo

			if err := json.Unmarshal(msg.Value, &dataSourceInfo); err != nil {
				fmt.Printf("Error parsing JSON: %v\n", err)
				continue
			}

			var value interface{}

			switch dataSourceInfo.DataType {
			case "recordCount":
				var intValue int
				if err := json.Unmarshal(dataSourceInfo.Payload, &intValue); err != nil {
					fmt.Printf("Error unmarshalling Payload as int: %v\n", err)
					continue
				}
				value = intValue
				fmt.Printf("DataSourceInfo. Resource: %d, DataStore: %d, Type: %s, Value: %d\n", dataSourceInfo.ResourceId, dataSourceInfo.DataStoreId, dataSourceInfo.DataType, intValue)

				p := influxdb2.NewPointWithMeasurement("RecordCount").
					AddTag("resourceId", strconv.Itoa(dataSourceInfo.ResourceId)).
					AddTag("dataStoreId", strconv.Itoa(dataSourceInfo.DataStoreId)).
					AddField("payload", value).
					SetTime(time.Now())
				err = recordCountWriteApi.WritePoint(context.Background(), p)

				if err != nil {
					fmt.Printf("Error writing to InfluxDB: %v\n", err)
				}

			case "dataStoreVolume":
				var floatValue float64
				if err := json.Unmarshal(dataSourceInfo.Payload, &floatValue); err != nil {
					fmt.Printf("Error unmarshalling Payload as float64: %v\n", err)
					continue
				}
				value = floatValue
				fmt.Printf("DataSourceInfo. Resource: %d, DataStore: %d, Type: %s, Value: %f\n", dataSourceInfo.ResourceId, dataSourceInfo.DataStoreId, dataSourceInfo.DataType, floatValue)

				p := influxdb2.NewPointWithMeasurement("DataStoreVolume").
					AddTag("resourceId", strconv.Itoa(dataSourceInfo.ResourceId)).
					AddTag("dataStoreId", strconv.Itoa(dataSourceInfo.DataStoreId)).
					AddField("payload", value).
					SetTime(time.Now())
				err = recordCountWriteApi.WritePoint(context.Background(), p)

				if err != nil {
					fmt.Printf("Error writing to InfluxDB: %v\n", err)
				}

			case "indexSize":
				var floatValue float64
				if err := json.Unmarshal(dataSourceInfo.Payload, &floatValue); err != nil {
					fmt.Printf("Error unmarshalling Payload as float64: %v\n", err)
					continue
				}
				value = floatValue
				fmt.Printf("DataSourceInfo. Resource: %d, DataStore: %d, Type: %s, Value: %f\n", dataSourceInfo.ResourceId, dataSourceInfo.DataStoreId, dataSourceInfo.DataType, floatValue)

				p := influxdb2.NewPointWithMeasurement("IndexSize").
					AddTag("resourceId", strconv.Itoa(dataSourceInfo.ResourceId)).
					AddTag("dataStoreId", strconv.Itoa(dataSourceInfo.DataStoreId)).
					AddField("payload", value).
					SetTime(time.Now())
				err = recordCountWriteApi.WritePoint(context.Background(), p)

				if err != nil {
					fmt.Printf("Error writing to InfluxDB: %v\n", err)
				}
			default:
				fmt.Println("Unsupported DataType")
				continue
			}
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
