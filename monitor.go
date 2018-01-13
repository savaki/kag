package kag

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/savaki/franz"
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

func (m *Monitor) fetchOffsets(conn *franz.Conn, nodeID int32, metadata *franz.MetadataResponseV0) (map[string]map[int32]int64, error) {
	input := makeListOffsetRequestV1(nodeID, metadata)
	resp, err := conn.ListOffsetsV1(input)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list offsets for broker, %v", conn.RemoteAddr())
	}

	offsets := map[string]map[int32]int64{}
	for _, t := range resp.Responses {
		offsetsByPartition, ok := offsets[t.Topic]
		if !ok {
			offsetsByPartition = map[int32]int64{}
			offsets[t.Topic] = offsetsByPartition
		}

		for _, p := range t.PartitionResponses {
			offsetsByPartition[p.Partition] = p.Offset
		}
	}

	return offsets, nil
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

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		encoder.Encode(groupOffsets)
		encoder.Encode(topicOffsets)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Minute):
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
		config.Brokers = []string{"localhost:9092"}
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
