package finder

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var log = logrus.WithFields(logrus.Fields{"package": "finder"})

var (
	ErrorContextCanceled = fmt.Errorf("parent context canceled")
)

type (
	Service struct {
		conf     Config
		stopOnce *sync.Once
		pfWg     *sync.WaitGroup
		pfMap    map[int]*partitionFinder
	}

	Config struct {
		Kafka Kafka
	}

	Kafka struct {
		Addresses []string
		Topic     string
	}
)

func NewService(ctx context.Context, conf Config) (s *Service, err error) {
	kafkaAddrs := strings.Join(conf.Kafka.Addresses, ",")
	conn, err := kafka.DialContext(ctx, "tcp", kafkaAddrs)
	if err != nil {
		return nil, fmt.Errorf("failed to dial to Kafka (addr=%q): %w", kafkaAddrs, err)
	}
	defer conn.Close()
	pfWg := &sync.WaitGroup{}

	parts, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to list all partitions: %w", err)
	}

	pfMap := make(map[int]*partitionFinder, len(parts))
	for _, p := range parts {
		if p.Topic != conf.Kafka.Topic {
			continue
		}
		// Start a new worker to work specifically on this partition only
		pfWg.Add(1)
		pf, err := newPartitionFinder(pfWg, conf.Kafka, p)
		if err != nil {
			return nil, fmt.Errorf("failed to create partition finder on (topic=%s, partition=%d): %w", conf.Kafka.Topic, p.ID, err)
		}
		pfMap[p.ID] = pf
		// log.Debugf("pf#%d: %#v", p.ID, pf)
	}

	return &Service{
		conf:     conf,
		stopOnce: &sync.Once{},
		pfMap:    pfMap,
		pfWg:     pfWg,
	}, nil
}

func (s *Service) GetMessageAt(ctx context.Context, t time.Time) (msgs []kafka.Message, err error) {
	errChan := make(chan error, len(s.pfMap))
	msgResolvedChan := make(chan kafka.Message, len(s.pfMap))
	defer func() {
		close(msgResolvedChan)
		close(errChan)
	}()

	pfCtx, pfCtxCancel := context.WithCancel(ctx)
	queryWg := &sync.WaitGroup{}
	for _, pf := range s.pfMap {
		queryWg.Add(1)
		go pf.getMessageAt(pfCtx, queryWg, t, msgResolvedChan, errChan) // Non-blocking
	}

	resolvedNum := 0
	msgs = make([]kafka.Message, 0, len(s.pfMap))
	for {
		select {
		case <-ctx.Done():
			// Caller decided to stop this call as it has been blocking for too long.
			// => Ask partition finder workers to abort resolving this message.
			pfCtxCancel()
			queryWg.Wait()
			return nil, ErrorContextCanceled
		case msg := <-msgResolvedChan:
			// Partition worker successfully resolved a message.
			// => Hold it until all partitions resolved.
			resolvedNum++
			msgs = append(msgs, msg)
			if resolvedNum == len(s.pfMap) {
				return msgs, nil
			}
		case err = <-errChan:
			// A partition finder returns error while resolving the message.
			// => Fail-fast
			pfCtxCancel()
			queryWg.Wait()
			return nil, err
		}
	}
}

func (s *Service) Stop() {
	s.stopOnce.Do(func() {
		if len(s.pfMap) <= 0 {
			return
		}

		for id, pf := range s.pfMap {
			log.Debugf("stopping partition finder #%d...", id)
			if err := pf.stop(); err != nil {
				log.Errorf("failed to safely stop partition finder #%d, will continue closing others: %s", id, err)
				continue
			}
			log.Debugf("partition finder #%d stopped gracefully", id)
		}

		// Wait until all partition finder workers has stopped safely
		s.pfWg.Wait()
	})
}
