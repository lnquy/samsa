package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/lnquy/samsa/pkg/condition"
	"github.com/lnquy/samsa/pkg/finder"
)

var log = logrus.WithFields(logrus.Fields{"package": "main"})

var (
	fKafkaAddr  string
	fKafkaTopic string
	fFromTime   string
	fToTime     string
	fCondition  string
	fDataType   string
)

func init() {
	flag.StringVar(&fKafkaAddr, "broker", "localhost:9092", "Address to Kafka broker(s), comma separated")
	flag.StringVar(&fKafkaTopic, "topic", "", "Which topic to search on?")

	flag.StringVar(&fFromTime, "from", "", "The beginning of time range to search")
	flag.StringVar(&fToTime, "to", "", "The end of time range to search")
	flag.StringVar(&fCondition, "cond", "", "Field condition to search for a message")
	flag.StringVar(&fDataType, "type", "", "Data type of Kafka messages (plaintext, json)")
}

func main() {
	flag.Parse()
	logrus.SetLevel(logrus.DebugLevel)

	cond, err := condition.Parse(fCondition, fDataType)
	panicIfErr(err, "failed to parse condition/dataType: %s", err)

	glbCtx, glbCtxCancel := context.WithCancel(context.Background())
	defer glbCtxCancel()

	svc, err := finder.NewService(glbCtx, finder.Config{
		Kafka: finder.Kafka{
			Addresses: strings.Split(fKafkaAddr, ","),
			Topic:     fKafkaTopic,
		},
	})
	panicIfErr(err, "failed to create finder service")
	// defer svc.Stop()

	fromTime, err := time.Parse(time.RFC3339, fFromTime)
	panicIfErr(err, "failed to parse fromTime: %s", fFromTime)
	toTime, err := time.Parse(time.RFC3339, fToTime)
	panicIfErr(err, "failed to parse toTime: %s", fToTime)

	start := time.Now()
	offsetMap, err := svc.ResolveBoundOffsets(glbCtx, fromTime, toTime)
	panicIfErr(err, "failed to resolve bound offsets")
	resolvedBoundOffsetsDuration := time.Since(start)
	log.Infof("offsets (d=%v): %v", resolvedBoundOffsetsDuration, offsetMap)

	start = time.Now()
	foundMsg, err := svc.FindMessageInBoundOffsets(glbCtx, offsetMap, cond)
	resolvedMessageDuration := time.Since(start)
	if err != nil {
		if errors.Is(err, finder.ErrorNotFound) {
			log.Errorf("not found message in range [%s, %s] with cond=%q, executionTime=%s", fromTime.Format(time.RFC3339), toTime.Format(time.RFC3339), fCondition, resolvedMessageDuration)
			return
		}
		log.Panicf("failed to find message in range [%s, %s]: %s", fromTime.Format(time.RFC3339), toTime.Format(time.RFC3339), err)
	}

	log.Infof("found message at topic=%s, partition=%d, offset=%d, timestamp=%s (d=%s): value=%s", foundMsg.Topic, foundMsg.Partition, foundMsg.Offset, foundMsg.Time, resolvedMessageDuration, foundMsg.Value)
}

func panicIfErr(err error, tmpl string, args ...interface{}) {
	if err == nil {
		return
	}
	if tmpl == "" {
		log.Panic(err)
	}
	log.Panic(fmt.Sprintf(tmpl, args...) + ": " + err.Error())
}

func writeTestData() {
	// make a writer that produces to topic-A, using the least-bytes distribution
	w := &kafka.Writer{
		Addr:     kafka.TCP(fKafkaAddr),
		Topic:    fKafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	startTime, err := time.Parse(time.RFC3339, "2021-06-01T00:00:00Z")
	panicIfErr(err, "failed to parse startTime")
	// Plaintext
	messages := make([]kafka.Message, 0, 10000)
	for i := 0; i < 3000000; i++ {
		if i != 0 && i%10000 == 0 {
			log.Printf("sending message#%d\n", i)
			err := w.WriteMessages(context.Background(), messages...)
			panicIfErr(err, "failed to write message#%d", i)
			log.Printf("message#%d written\n", i)
			messages = make([]kafka.Message, 0, 10000)
			continue
		}

		b := []byte(strconv.Itoa(i))
		messages = append(messages, kafka.Message{
			Time:  startTime.Add(time.Duration(i) * time.Second),
			Key:   b,
			Value: b,
		})
	}

	// JSON
	messages = make([]kafka.Message, 0, 10000)
	for i := 0; i < 100000; i++ {
		if i != 0 && i%10000 == 0 {
			log.Printf("sending message#%d\n", i)
			err := w.WriteMessages(context.Background(), messages...)
			panicIfErr(err, "failed to write message#%d", i)
			log.Printf("message#%d written\n", i)
			messages = make([]kafka.Message, 0, 10000)
			continue
		}

		b := []byte(strconv.Itoa(i))

		messages = append(messages, kafka.Message{
			Time:  startTime.Add(time.Duration(3000000+i) * time.Second),
			Key:   b,
			Value: []byte(fmt.Sprintf(`{"string":"Hello World","object":{"id":"%d","number":%d}}`, i, i)),
		})
	}
}
