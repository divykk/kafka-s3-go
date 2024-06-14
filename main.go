/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This is a simple example demonstrating how to produce a message to
// a topic, and then reading it back again using a consumer. The topic
// belongs to a Apache Kafka cluster from Confluent Cloud. For more
// information about Confluent Cloud, please visit:
//
// https://www.confluent.io/confluent-cloud/
// Kafka

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2/ocf"
	"github.com/hamba/avro/v2/registry"
)

const (
	bootstrapServers          = `http://localhost:29092/`
	schemaRegistryAPIEndpoint = `http://192.168.31.102:8081/`
)

// func main() {
// 	client, _ := registry.NewClient(schemaRegistryAPIEndpoint, registry.WithBasicAuth(schemaRegistryAPIKey, schemaRegistryAPISecret))
// 	decoder := registry.NewDecoder(client)

// 	// Now consumes the record and print its value...
// 	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
// 		"bootstrap.servers":        bootstrapServers,
// 		"sasl.mechanisms":          "PLAIN",
// 		"security.protocol":        "SASL_SSL",
// 		"sasl.username":            ccloudAPIKey,
// 		"sasl.password":            ccloudAPISecret,
// 		"session.timeout.ms":       6000,
// 		"group.id":                 "Lambda-1",
// 		"auto.offset.reset":        "earliest",
// 		"enable.auto.offset.store": false,
// 		"enable.auto.commit":       false,
// 		"max.poll.interval.ms":     8000})

// 	if err != nil {
// 		panic(fmt.Sprintf("Failed to create consumer: %s", err))
// 	}
// 	fmt.Println("Consumer Created")

// consumer.SubscribeTopics(topics, rebalanceCallback)
// 	consumer.Subscribe(topicConsumption, rebalanceCallback)
// 	defer consumer.Close()

// 	count := 0
// 	for {
// 		fmt.Println("Getting Messages")
// 		message, err := consumer.ReadMessage(100 * time.Millisecond)
// 		if err == nil {
// 			//received := CXVirtualAccount{}
// 			var got any
// 			schemaID, err := extractSchemaID(message.Value)
// 			if err != nil {
// 				fmt.Printf("Failed to deserialize payload: %s\n", err)
// 			}
// 			err = decoder.Decode(context.Background(), message.Value, &got)
// 			//err := deser.DeserializeInto(*message.TopicPartition.Topic, message.Value, &received)
// 			if err != nil {
// 				fmt.Printf("Failed to deserialize payload: %s\n", err)
// 			} else {
// 				fmt.Printf("consumed from topic %s [%d] at offset %v: %+v",
// 					*message.TopicPartition.Topic,
// 					message.TopicPartition.Partition, message.TopicPartition.Offset,
// 					got)
// 				fmt.Println()
// 				fmt.Println("Record")
// 				//fmt.Println(*got.Mlb.MlbComponent.ComponentMaximumQty)
// 				filename := *message.TopicPartition.Topic + strconv.Itoa(int(message.TopicPartition.Partition)) + "-" + strconv.Itoa(int(message.TopicPartition.Offset)) + ".avro"
// 				fmt.Println(filename)
// 				// f, err := os.Open(filename)
// 				f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
// 				if err != nil {
// 					log.Fatal(err)
// 				}
// 				schema, err := client.GetSchema(context.Background(), schemaID)
// 				if err != nil {
// 					log.Fatal(err)
// 				}
// 				enc, err := ocf.NewEncoder(schema.String(), f)
// 				if err != nil {
// 					log.Fatal(err)
// 				}
// 				// var record CxVirtualAccount
// 				err = enc.Encode(got)
// 				if err != nil {
// 					log.Fatal(err)
// 				}

// 				if err := enc.Flush(); err != nil {
// 					log.Fatal(err)
// 				}

//					if err := f.Sync(); err != nil {
//						log.Fatal(err)
//					}
//					f.Close()
//					count++
//					if count == 3 {
//						break
//					}
//				}
//			}
//		}
//	}

var (
	client       *registry.Client
	schemaID     int
	filecounter  atomic.Uint32
	consumer     *kafka.Consumer
	consumerLock *sync.Mutex
)

func getFilename(number int) string {
	filecounter.Add(1)
	return fmt.Sprintf("file-%d-%d.avro", number, filecounter.Load())
}

func Worker(records <-chan *kafka.Message, done <-chan bool, timeToresetfile int, countRecords int, wg *sync.WaitGroup, number int) {
	// filename := fmt.Sprintf("output_%d.avro", time.Now().Unix())
	// f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// enc, err := ocf.NewEncoder(schema.String(), f)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// for record := range records {
	// 	_, err = enc.Write(record)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
	count := 0
	var enc *ocf.Encoder
	var f *os.File
	filename := getFilename(number)
	log.Printf("got filename first %s", filename)
	enc, f, err := getEncoder(filename)
	if err != nil {
		log.Println("Error getting encoder for first time")
		wg.Done()
		return
	}
	offsets := make([]kafka.TopicPartition, 0)
	ticker := time.NewTicker(time.Duration(timeToresetfile) * time.Minute)
	for {
		log.Printf("Worker-%d-in-for-loop", number)
		select {
		case message, ok := <-records:
			if !ok {
				if err := enc.Close(); err != nil {
					log.Println("Error closing Encode")
				}
				f.Close()
				log.Printf("offsets to commit: %v", offsets)
				consumerLock.Lock()
				committedOffsets, err := consumer.CommitOffsets(offsets)
				if err != nil {
					log.Printf("Error commiting offsets: %s", err)
				}
				log.Printf("offsets commited: %v", committedOffsets)
				consumerLock.Unlock()
				log.Println(slices.Equal(committedOffsets, offsets))
				offsets = slices.Delete(offsets, 0, len(offsets))
				log.Println("closing Encoder goroutine")
				wg.Done()
				return
			}
			if count >= countRecords {
				log.Printf("Completed Writing %d records in %d worker", countRecords, number)
				if err := enc.Close(); err != nil {
					log.Println("Error closing Encoder while getting new")
				}
				f.Close()
				consumerLock.Lock()
				consumer.CommitOffsets(offsets)
				consumerLock.Unlock()
				offsets = slices.Delete(offsets, 0, len(offsets))
				filename = getFilename(number)
				log.Printf("got filename count %s", filename)
				enc, f, err = getEncoder(filename)
				if err != nil {
					log.Println("Error getting encoder")
				}
				count = 0
			}
			record := message.Value
			_, err := enc.Write(record[5:])
			if err != nil {
				log.Printf("Error writing to buffers %s", err)
				if err := enc.Close(); err != nil {
					log.Println("Error closing Encoder after write error")
				}
				f.Close()
				consumerLock.Lock()
				consumer.CommitOffsets(offsets)
				consumerLock.Unlock()
				offsets = slices.Delete(offsets, 0, len(offsets))
				wg.Done()
				return
			}
			offsets = append(offsets, message.TopicPartition)
			// consumerLock.Lock()
			// consumer.StoreMessage(message)
			// consumerLock.Unlock()
			log.Printf("Done writing Message with Offset %v", message.TopicPartition.Offset)
			count++
		case <-ticker.C:
			if err := enc.Close(); err != nil {
				log.Println("Error closing Encoder while getting new timer")
			}
			f.Close()
			consumerLock.Lock()
			consumer.CommitOffsets(offsets)
			consumerLock.Unlock()
			offsets = slices.Delete(offsets, 0, len(offsets))
			filename = getFilename(number)
			log.Printf("got filename timer %s", filename)
			enc, f, err = getEncoder(filename)
			if err != nil {
				log.Println("Error getting encoder")
				wg.Done()
				return
			}
			count = 0
		}
		log.Printf("Worker-%d-out-for-loop", number)
	}
}

func getEncoder(filename string) (*ocf.Encoder, *os.File, error) {
	var enc *ocf.Encoder
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Println("Unable to get File Handler")
		f.Close()
		return nil, nil, err
	}
	schema, err := client.GetSchema(context.Background(), 1)
	if err != nil {
		log.Printf("Unable to get Schema %s", err)
		f.Close()
		return nil, nil, err
	}
	enc, err = ocf.NewEncoder(schema.String(), f)
	if err != nil {
		log.Printf("Unable to get Encoder %s", err)
		f.Close()
		return nil, nil, err
	}
	return enc, f, nil
}

func main() {
	// runtime.GOMAXPROCS(8)
	// fmt.Printf("Using %d CPU cores\n", 8)
	// client, _ = registry.NewClient(schemaRegistryAPIEndpoint, registry.WithBasicAuth(schemaRegistryAPIKey, schemaRegistryAPISecret))
	client, _ = registry.NewClient(schemaRegistryAPIEndpoint)
	// decoder := registry.NewDecoder(client)
	var err error
	consumerLock = &sync.Mutex{}
	consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		// "sasl.mechanisms":          "PLAIN",
		// "security.protocol":        "SASL_SSL",
		// "sasl.username":            ccloudAPIKey,
		// "sasl.password":            ccloudAPISecret,
		"session.timeout.ms":       6000,
		"group.id":                 "kafka-go",
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
		"enable.auto.commit":       false,
		// "debug":                    "all",
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %s", err))
	}
	fmt.Println("Consumer Created")
	// consumer.SubscribeTopics(topics, rebalanceCallback)
	topicLocal := "pageviews"
	consumer.Subscribe(topicLocal, nil)
	// records := make(chan []byte)
	records := make(chan *kafka.Message)
	done := make(chan bool)

	osSigns := make(chan os.Signal, 1)
	signal.Notify(osSigns, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	var wg sync.WaitGroup
	for i := 1; i < 4; i++ {
		wg.Add(1)
		// offsets := make([]kafka.TopicPartition, 0)
		go Worker(records, done, 30, 100, &wg, i)
	}

	count := 0
	timeNow := time.Now()
	for {
		select {
		case <-osSigns:
			close(records)
			log.Println("Waiting for Go routines to stop")
			wg.Wait()
			consumer.Close()
			return
		default:
			fmt.Println("Getting Messages")
			message, err := consumer.ReadMessage(-1)
			if err != nil {
				close(records)
				log.Printf("Unbable to read records: %s", err)
				log.Println("Waiting for Go routines to stop")
				wg.Wait()
				consumer.Close()
				log.Println("Completed Go routines to stop")
				log.Printf("Duration:%v", time.Since(timeNow))
				return
			}
			log.Println("got one")
			if count == 0 {
				schemaID, err = extractSchemaID(message.Value)
				if err != nil {
					log.Printf("Error Extracting SchemaID %s", err)
				}
				log.Printf("schemaID %d", schemaID)
			}
			// if err == nil {
			// 	schemaID, err = extractSchemaID(message.Value)
			// 	if err != nil {
			// 		fmt.Printf("Failed to deserialize payload: %s\n", err)
			// 	}
			// }
			// filename := *message.TopicPartition.Topic + strconv.Itoa(int(message.TopicPartition.Partition)) + "-" + strconv.Itoa(int(message.TopicPartition.Offset)) + ".avro"
			// record := make([]byte, len(message.Value[5:]))
			// copy(record, message.Value[5:])
			// record := message.Value
			records <- message
			// consumerLock.Lock()
			// consumer.StoreMessage(message)
			// consumerLock.Unlock()
			count++
			//offsets = append(offsets, int(message.TopicPartition.Offset))
			if count == 1 {
				close(records)
				log.Printf("Stopping reads reading %v", count)
				log.Println("Waiting for Go routines to stop")
				wg.Wait()
				consumer.Close()
				log.Println("Completed Go routines to stop")
				log.Printf("Duration:%v", time.Since(timeNow))
				return
			}
		}
	}
}

func extractSchemaID(data []byte) (int, error) {
	if len(data) < 5 {
		return 0, errors.New("data too short")
	}
	if data[0] != 0 {
		return 0, fmt.Errorf("invalid magic byte: %x", data[0])
	}
	return int(binary.BigEndian.Uint32(data[1:5])), nil
}

func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		fmt.Printf("%% %s rebalance: %d new partition(s) assigned: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

		// The application may update the start .Offset of each assigned
		// partition and then call Assign(). It is optional to call Assign
		// in case the application is not modifying any start .Offsets. In
		// that case we don't, the library takes care of it.
		// It is called here despite not modifying any .Offsets for illustrative
		// purposes.

		err := c.Assign(ev.Partitions)
		if err != nil {
			return err
		}

	case kafka.RevokedPartitions:

		fmt.Printf("%% %s rebalance: %d partition(s) revoked: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

		// Usually, the rebalance callback for `RevokedPartitions` is called
		// just before the partitions are revoked. We can be certain that a
		// partition being revoked is not yet owned by any other consumer.
		// This way, logic like storing any pending offsets or committing
		// offsets can be handled.
		// However, there can be cases where the assignment is lost
		// involuntarily. In this case, the partition might already be owned
		// by another consumer, and operations including committing
		// offsets may not work.
		if c.AssignmentLost() {
			// Our consumer has been kicked out of the group and the
			// entire assignment is thus lost.
			fmt.Fprintln(os.Stderr, "Assignment lost involuntarily, commit may fail")
		}

		// Since enable.auto.commit is unset, we need to commit offsets manually
		// before the partition is revoked.
		commitedOffsets, err := c.Commit()
		if err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
			fmt.Fprintf(os.Stderr, "Failed to commit offsets: %s\n", err)
			return err
		}
		fmt.Printf("%% Commited offsets to Kafka: %v\n", commitedOffsets)

		// Similar to Assign, client automatically calls Unassign() unless the
		// callback has already called that method. Here, we don't call it.

	default:
		fmt.Fprintf(os.Stderr, "Unxpected event type: %v\n", event)
	}

	return nil
}
