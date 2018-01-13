package kag

import (
	"fmt"

	"github.com/savaki/franz"
)

func findAddr(nodeID int32, metadata *franz.MetadataResponseV0) (string, bool) {
	for _, broker := range metadata.Brokers {
		if broker.NodeID == nodeID {
			return fmt.Sprintf("%v:%v", broker.Host, broker.Port), true
		}
	}

	return "", false
}

func makeTopics(in []*franz.MetadataResponseV0Topic) []franz.OffsetFetchRequestV3Topic {
	var topics []franz.OffsetFetchRequestV3Topic

	for _, topic := range in {
		item := franz.OffsetFetchRequestV3Topic{
			Topic: topic.TopicName,
		}
		for _, partition := range topic.Partitions {
			item.Partitions = append(item.Partitions, partition.PartitionID)
		}

		topics = append(topics, item)
	}
	return topics
}

func makeListOffsetRequestV1(nodeID int32, metadata *franz.MetadataResponseV0) franz.ListOffsetRequestV1 {
	out := franz.ListOffsetRequestV1{}

	for _, topic := range metadata.Topics {
		if topic.TopicName == "__consumer_offsets" {
			continue
		}

		item := franz.ListOffsetRequestV1Topic{
			TopicName: topic.TopicName,
		}

		for _, partition := range topic.Partitions {
			if partition.Leader != nodeID {
				continue
			}

			item.Partitions = append(item.Partitions, franz.ListOffsetRequestV1Partition{
				Partition: partition.PartitionID,
				Time:      -1,
			})
		}

		if len(item.Partitions) > 0 {
			out.Topics = append(out.Topics, item)
		}
	}

	return out
}

func removeEmpty(in []franz.OffsetFetchResponseV3Response) (responses []franz.OffsetFetchResponseV3Response) {
	for _, item := range in {
		present := false
		for _, partition := range item.PartitionResponses {
			if partition.Offset != -1 {
				present = true
				break
			}
		}
		if !present {
			continue
		}

		responses = append(responses, item)
	}
	return
}
