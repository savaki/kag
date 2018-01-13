package kag

import (
	"context"

	"github.com/pkg/errors"
	"github.com/savaki/franz"
	"golang.org/x/sync/errgroup"
)

type broker struct {
	nodeID int32
	conn   *franz.Conn
}

func (b *broker) fetchTopicOffsets(metadata *franz.MetadataResponseV0) (topicOffsets, error) {
	input := makeListOffsetRequestV1(b.nodeID, metadata)
	resp, err := b.conn.ListOffsetsV1(input)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list offsets for broker, %v", b.conn.RemoteAddr())
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

func (b *broker) fetchGroupOffsets(topics []franz.OffsetFetchRequestV3Topic) (groupOffsets, error) {
	resp, err := b.conn.ListGroupsV1(franz.ListGroupsRequestV1{})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list groups for broker, %v", b.conn.RemoteAddr())
	}

	offsets := groupOffsets{}

	for _, group := range resp.Groups {
		offsetFetch, err := b.conn.OffsetFetchV3(franz.OffsetFetchRequestV3{
			GroupID: group.GroupID,
			Topics:  topics,
		})
		if err != nil {
			return nil, errors.Wrapf(err, "unable to fetch offset for consumer group, %v", group.GroupID)
		}

		for _, r := range removeEmpty(offsetFetch.Responses) {
			for _, pr := range r.PartitionResponses {
				offsets.add(group.GroupID, r.Topic, pr.Partition, pr.Offset)
			}
		}
	}

	return offsets, nil
}

func (b *broker) Close() error {
	return b.conn.Close()
}

func newBroker(nodeID int32, conn *franz.Conn) *broker {
	return &broker{
		nodeID: nodeID,
		conn:   conn,
	}
}

type brokerArray []*broker

func (b brokerArray) fetchTopicOffsets(ctx context.Context, metadata *franz.MetadataResponseV0) (topicOffsets, error) {
	results := make(chan topicOffsets, len(b))

	group := &errgroup.Group{}
	for _, broker := range b {
		group.Go(func() error {
			offsets, err := broker.fetchTopicOffsets(metadata)
			if err == nil {
				results <- offsets
			}
			return err
		})
	}
	group.Wait()
	close(results)

	all := topicOffsets{}
	for offset := range results {
		for topic, partitions := range offset {
			for partition, offset := range partitions {
				all.add(topic, partition, offset)
			}
		}
	}

	return all, nil
}

func (b brokerArray) fetchGroupOffsets(ctx context.Context, metadata *franz.MetadataResponseV0) (groupOffsets, error) {
	results := make(chan groupOffsets, len(b))

	topics := makeTopics(metadata.Topics)

	group := &errgroup.Group{}
	for _, broker := range b {
		group.Go(func() error {
			offsets, err := broker.fetchGroupOffsets(topics)
			if err == nil {
				results <- offsets
			}
			return err
		})
	}
	group.Wait()
	close(results)

	all := groupOffsets{}
	for offset := range results {
		for groupID, topics := range offset {
			for topic, partitions := range topics {
				for partition, offset := range partitions {
					all.add(groupID, topic, partition, offset)
				}
			}
		}
	}

	return all, nil
}

func (b brokerArray) Close() (err error) {
	for _, broker := range b {
		if v := broker.Close(); v != nil {
			err = v
		}
	}
	return
}
