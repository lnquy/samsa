package finder

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	ErrorBeforeFirstMessageTime = fmt.Errorf("the given time is before the first message time")
	ErrorAfterLastMessageTime   = fmt.Errorf("the given time is after the last message time")
	ErrorNotFound               = fmt.Errorf("not found")
	ErrorAllMessagesFetched     = fmt.Errorf("all messages fetched")

	// poisonPill is used to mark the last message has been sent into the channel.
	poisonPill = kafka.Message{
		Partition:     -1,
		Offset:        -1,
	}
)

type partitionFinder struct {
	conf        Kafka
	partitionId int
	conn        *kafka.Conn

	wg       *sync.WaitGroup
	stopOnce *sync.Once

	firstMsg kafka.Message
	lastMsg  kafka.Message
}

func newPartitionFinder(wg *sync.WaitGroup, conf Kafka, partition kafka.Partition) (pf *partitionFinder, err error) {
	conn, err := kafka.DialPartition(context.Background(), "tcp", conf.Addresses[0], partition)
	if err != nil {
		return nil, fmt.Errorf("failed to dial to Kafka (addr=%q): %w", conf.Addresses[0], err)
	}

	firstMsg, lastMsg, err := fetchBoundMessages(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to fetchBoundMessages: %w", err)
	}
	// log.Infof("%s#%d:\nfirstMsg=%#v\nlastMsg=%#v", conf.Topic, partition.ID, firstMsg, lastMsg)

	return &partitionFinder{
		wg:          wg,
		conf:        conf,
		partitionId: partition.ID,
		conn:        conn,
		stopOnce:    &sync.Once{},
		firstMsg:    firstMsg,
		lastMsg:     lastMsg,
	}, nil
}

func (pf *partitionFinder) stop() (err error) {
	pf.stopOnce.Do(func() {
		pf.wg.Done()

		if pf.conn == nil {
			return
		}
		err = pf.conn.Close()
	})

	return err
}

func (pf *partitionFinder) refreshBoundMessages() error {
	firstMsg, lastMsg, err := fetchBoundMessages(pf.conn)
	if err != nil {
		return err
	}
	pf.firstMsg = firstMsg
	pf.lastMsg = lastMsg
	return nil
}

// getMessageAt resolves the first message with time after the given t.
func (pf *partitionFinder) getMessageAt(ctx context.Context, wg *sync.WaitGroup, t time.Time, msgChan chan<- kafka.Message, errChan chan<- error) {
	defer wg.Done()

	// Fast cases, when we can resolve the message without calling to Kafka or only call 1 time
	select {
	case <-ctx.Done():
		// Caller decided to stop resolving this message, just end asap.
		return
	default:
		// Try to resolve the message from Kafka topic's partition.
		// 1. Try to refresh the firstMsg/lastMsg in partition now as there's a chance that new messages
		// has been pushed into the partition since the time we create the partition finder.
		// Or Kafka log retention deleted the old firstMessage already.
		if err := pf.refreshBoundMessages(); err != nil {
			errChan <- fmt.Errorf("failed to refreshBoundMessage: %w", err)
			return
		}
		// 2. If the given t is before the first message then we cannot find the message.
		if t.Before(pf.firstMsg.Time) {
			errChan <- ErrorBeforeFirstMessageTime // Non-blocking
			return
		}
		// 3. If the given t is after the last message then we can be sure we cannot find the message.
		if t.After(pf.lastMsg.Time) {
			errChan <- ErrorAfterLastMessageTime
			return
		}
		// 4. The given time t is in the range of [firstMsg, lastMsg] now, we can definitely
		// resolve the message.
		// => In case there's only 1 message in the partition then return it.
		if pf.firstMsg.Offset == pf.lastMsg.Offset {
			msgChan <- pf.firstMsg
			return
		}
		// If cannot resolve the message in happy (exceptional) cases then we have to do the normal
		// "boring" search below
	}

	// leftOffset := pf.firstMsg.Offset
	// rightOffset := pf.lastMsg.Offset
	// // TODO: Picking better mid offset by calculating how relatively t in the range of [firstMsg.Time, lastMsg.Time]
	// midOffset := (leftOffset + rightOffset) / 2

	// Normal case when we will have to call to Kafka multiple times (ln(lastMsg.Offset - firstMsg.Offset))
	// to resolve the message
	for {
		select {
		case <-ctx.Done():
			// Caller decided to stop resolving this message, just end asap.
			return
		default:
			// Call to ListOffsetRequest on partition timestamp index O(1)
			offset, err := pf.conn.ReadOffset(t)
			if err != nil {
				errChan <- fmt.Errorf("failed to read offet at t=%s: %w", t.Format(time.RFC3339), err)
				return
			}
			if _, err := pf.conn.Seek(offset, kafka.SeekAbsolute); err != nil {
				errChan <- fmt.Errorf("failed to seek offset=%d: %w", offset, err)
				return
			}
			msg, err := pf.conn.ReadMessage(10e3)
			if err != nil {
				errChan <- fmt.Errorf("failed to read message at offset=%d: %w", offset, err)
				return
			}
			msgChan <- msg
			return
		}
	}
}

func (pf *partitionFinder) fetchMessagesInRange(ctx context.Context, wg *sync.WaitGroup, fromOffset, toOffset int64, msgChan chan<- kafka.Message, errChan chan<- error) {
	defer wg.Done()

	if _, err := pf.conn.Seek(fromOffset, kafka.SeekAbsolute); err != nil {
		errChan <- fmt.Errorf("failed to seek fromOffset=%d: %w", fromOffset, err)
		return
	}
	// log.Debugf("pf#%d seeked to offset=%d", pf.partitionId, fromOffset)

	for {
		select {
		case <-ctx.Done():
			// Caller decided to stop resolving this message, just end asap.
			return
		default:
			// log.Debugf("pf#%d: default hit", pf.partitionId)
			// Fetch messages from Kafka by batch
			batch := pf.conn.ReadBatchWith(kafka.ReadBatchConfig{
				MinBytes: 10e3,
				MaxBytes: 10e6,
				MaxWait:  2 * time.Second,
			})
			// log.Debugf("pf#%d: read batch hit", pf.partitionId)

			// Read messages from the fetched batch.
		readBatchLoop:
			for {
				// Less efficient than Read()
				msg, err := batch.ReadMessage()
				if err != nil {
					if err == io.EOF {
						_ = batch.Close()
						// log.Debugf("pf#%d: break batch loop, lastOffset=%d", pf.partitionId, batch.Offset())
						break readBatchLoop // Continue to read another batch
					}
					_ = batch.Close()
					errChan <- fmt.Errorf("failed to read message offset=%d: %w", batch.Offset(), err)
					return
				}
				if msg.Offset > toOffset { // Stop fetching when we reached toOffset message
					_ = batch.Close()
					msgChan <- poisonPill // Notify that this is the last message that will be sent into the channel
					// log.Debugf("pf#%d: toOffset reached, toOffset=%d, msg.Offset=%d", pf.partitionId, toOffset, msg.Offset)
					return
				}

				// Message read successfully
				// log.Debugf("msg: %v", msg)
				msgChan <- msg
			}
		}
	}
}

func fetchBoundMessages(conn *kafka.Conn) (firstMsg, lastMsg kafka.Message, err error) {
	_msg := kafka.Message{}
	firstOffset, lastOffset, err := conn.ReadOffsets()
	if err != nil {
		return _msg, _msg, fmt.Errorf("failed to read offsets: %w", err)
	}

	if _, err := conn.Seek(firstOffset, kafka.SeekAbsolute); err != nil {
		return _msg, _msg, fmt.Errorf("failed to seek to firstOffset=%d: %w", firstOffset, err)
	}
	firstMsg, err = conn.ReadMessage(10e3)
	if err != nil {
		return _msg, _msg, fmt.Errorf("failed to fetch first message offset=%d: %w", firstOffset, err)
	}

	lastOffset-- // Because we need to read the last message
	if _, err := conn.Seek(lastOffset, kafka.SeekAbsolute); err != nil {
		return _msg, _msg, fmt.Errorf("failed to seek to lastOffset=%d: %w", lastOffset, err)
	}
	lastMsg, err = conn.ReadMessage(10e3)
	if err != nil {
		return _msg, _msg, fmt.Errorf("failed to fetch last message offset=%d: %w", lastOffset, err)
	}

	return firstMsg, lastMsg, nil
}
