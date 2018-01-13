package kag

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/savaki/franz"
)

type Observer interface {
	Observe(groupID, topic string, partition int32, lag int64)
}

type ObserverFunc func(groupID, topic string, partition int32, lag int64)

func (fn ObserverFunc) Observe(groupID, topic string, partition int32, lag int64) {
	fn(groupID, topic, partition, lag)
}

var (
	Stdout Observer = ObserverFunc(func(groupID, topic string, partition int32, lag int64) {
		fmt.Printf("%v/%v/%v => %v\n", groupID, topic, partition, lag)
	})
	Nop Observer = ObserverFunc(func(groupID, topic string, partition int32, lag int64) {})
)

type Monitor struct {
	cancel       context.CancelFunc
	done         chan struct{}
	err          error
	config       Config
	dialer       *franz.Dialer
	topicOffsets chan topicOffsets
	groupOffsets chan groupOffsets
}

func (m *Monitor) connectAny(ctx context.Context) (*franz.Conn, error) {
	for _, broker := range m.config.Brokers {
		if conn, err := m.dialer.DialContext(ctx, "tcp", broker); err == nil {
			return conn, nil
		}
	}

	return nil, errors.Errorf("unable to connect to any broker")
}

type ScanOut struct {
	Offsets map[string]map[int32]int64
}

func (m *Monitor) monitor(ctx context.Context) error {
	conn, err := m.connectAny(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	metadata, err := conn.MetadataV0(franz.MetadataRequestV0{})
	if err != nil {
		return errors.Wrapf(err, "unable to retrieve metadata")
	}

	brokers := brokerArray{}
	defer brokers.Close()

	for _, broker := range metadata.Brokers {
		addr := fmt.Sprintf("%v:%v", broker.Host, broker.Port)
		conn, err := m.dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return errors.Wrapf(err, "unable to connect to broker, %v", addr)
		}
		brokers = append(brokers, newBroker(broker.NodeID, conn))
	}

	brokerList := metadata.Brokers
	sort.Slice(brokerList, func(i, j int) bool { return brokerList[i].NodeID < brokerList[j].NodeID })

	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()

	for {
		metadata, err := conn.MetadataV0(franz.MetadataRequestV0{})
		if err != nil {
			return errors.Wrapf(err, "unable to retrieve metadata")
		}

		found := metadata.Brokers
		sort.Slice(found, func(i, j int) bool { return found[i].NodeID < found[j].NodeID })
		if !reflect.DeepEqual(brokerList, found) {
			return errors.Errorf("detected change in broker list")
		}

		groupOffsets, err := brokers.fetchGroupOffsets(ctx, metadata)
		if err != nil {
			return err
		}

		topicOffsets, err := brokers.fetchTopicOffsets(ctx, metadata)
		if err != nil {
			return err
		}

		for groupID, topics := range groupOffsets {
			for topic, partitions := range topics {
				offsetsByPartition, ok := topicOffsets[topic]
				if !ok {
					continue
				}

				for partition, offset := range partitions {
					v, ok := offsetsByPartition[partition]
					if !ok {
						continue
					}

					lag := v - offset
					if lag < 0 {
						lag = 0
					}
					m.config.Observer.Observe(groupID, topic, partition, lag)
				}
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}

	return nil
}

func (m *Monitor) run(ctx context.Context) {
	defer close(m.done)

	for {
		if err := m.monitor(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
		}
	}
}

func (m *Monitor) Close() error {
	m.cancel()
	<-m.done
	return m.err
}

func NewContext(ctx context.Context, config Config) *Monitor {
	if len(config.Brokers) == 0 {
		panic(errors.Errorf("Brokers not set"))
	}
	if config.Observer == nil {
		config.Observer = Nop
	}
	if config.Interval == 0 {
		config.Interval = DefaultInterval
	}

	dialer := &franz.Dialer{
		ClientID:      config.ClientID,
		Timeout:       config.Timeout,
		Deadline:      config.Deadline,
		LocalAddr:     config.LocalAddr,
		DualStack:     config.DualStack,
		FallbackDelay: config.FallbackDelay,
		KeepAlive:     config.KeepAlive,
		Resolver:      config.Resolver,
		TLS:           config.TLS,
	}

	ctx, cancel := context.WithCancel(ctx)
	m := &Monitor{
		cancel: cancel,
		done:   make(chan struct{}),
		dialer: dialer,
		config: config,
	}
	go m.run(ctx)

	return m
}

func New(config Config) *Monitor {
	return NewContext(context.Background(), config)
}
