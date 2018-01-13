package kag

type topicOffsets map[string]map[int32]int64

func (t topicOffsets) add(topic string, partition int32, offset int64) {
	offsetsByPartition, ok := t[topic]
	if !ok {
		offsetsByPartition = map[int32]int64{}
		t[topic] = offsetsByPartition
	}

	offsetsByPartition[partition] = offset
}

type groupOffsets map[string]topicOffsets

func (g groupOffsets) find(groupID string) topicOffsets {
	offsets, ok := g[groupID]
	if !ok {
		offsets = topicOffsets{}
		g[groupID] = offsets
	}
	return offsets
}

func (g groupOffsets) add(groupID, topic string, partition int32, offset int64) {
	g.find(groupID).add(topic, partition, offset)
}
