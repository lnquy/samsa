package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/lnquy/samsa/pkg/finder"
)

var log = logrus.WithFields(logrus.Fields{"package": "main"})

var (
	fFromTime  string
	fToTime    string
	fCondition string
)

func init() {
	flag.StringVar(&fFromTime, "from", "", "The beginning of time range to search")
	flag.StringVar(&fToTime, "to", "", "The end of time range to search")
	flag.StringVar(&fCondition, "cond", "", "Field condition to search for a message")
}

var (
	kafkaTopic = "test-samsa"
	kafkaAddr  = []string{"localhost:9091"}
)

func main() {
	flag.Parse()
	logrus.SetLevel(logrus.DebugLevel)

	glbCtx, glbCtxCancel := context.WithCancel(context.Background())
	defer glbCtxCancel()

	svc, err := finder.NewService(glbCtx, finder.Config{
		Kafka: finder.Kafka{
			Addresses:   kafkaAddr,
			Topic:     kafkaTopic,
		},
	})
	panicIfErr(err, "failed to create finder service")

	t, err := time.Parse(time.RFC3339, fFromTime)
	panicIfErr(err, "failed to parse from time: %s", fFromTime)
	startTime := time.Now()
	log.Infof("start finding message at %q", t.Format(time.RFC3339))
	msgs, err := svc.GetMessageAt(glbCtx, t)
	panicIfErr(err, "failed to get message at %q from Kafka", t.Format(time.RFC3339))
	log.Infof("resolved message at %q after %v", t.Format(time.RFC3339), time.Since(startTime))
	log.Infof("resolved messages: %+v", msgs)
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
		Addr:     kafka.TCP(strings.Join(kafkaAddr, ",")),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	startTime, err := time.Parse(time.RFC3339, "2021-06-01T00:00:00Z")
	panicIfErr(err, "failed to parse startTime")
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
}
