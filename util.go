package kag

import (
	"github.com/savaki/franz"
)

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

func observeLag(observer Observer, topicOffsets topicOffsets, groupOffsets groupOffsets) {
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
				if v == -1 {
					continue
				}

				lag := v - offset
				if lag < 0 {
					lag = 0
				}
				observer.Observe(groupID, topic, partition, lag)
			}
		}
	}
}
