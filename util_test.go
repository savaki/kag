package kag

import (
	"testing"

	"github.com/tj/assert"
)

func TestObserveLag(t *testing.T) {
	testCases := map[string]struct {
		Topics topicOffsets
		Groups groupOffsets
		Lag    map[string]map[string]map[int32]int64
	}{
		"empty": {
			Lag: map[string]map[string]map[int32]int64{},
		},
		"no consumers": {
			Topics: topicOffsets{
				"topic": {0: 123},
			},
			Lag: map[string]map[string]map[int32]int64{},
		},
		"1 consumer, no lag": {
			Groups: groupOffsets{
				"group": topicOffsets{
					"topic": {0: 123},
				},
			},
			Topics: topicOffsets{
				"topic": {0: 123},
			},
			Lag: map[string]map[string]map[int32]int64{
				"group": {
					"topic": {0: 0},
				},
			},
		},
		"1 consumer, 2 lag": {
			Groups: groupOffsets{
				"group": topicOffsets{
					"topic": {0: 121},
				},
			},
			Topics: topicOffsets{
				"topic": {0: 123},
			},
			Lag: map[string]map[string]map[int32]int64{
				"group": {
					"topic": {0: 2},
				},
			},
		},
		"complex, 0 lag": {
			Groups: groupOffsets{
				"vavende-email": {
					"master.identity.org-events": {
						0: -1,
						1: 10,
						2: -1,
					},
				},
			},
			Topics: topicOffsets{
				"master.identity.org-events": {
					0: -1,
					1: 10,
					2: -1,
				},
			},
			Lag: map[string]map[string]map[int32]int64{
				"vavende-email": {
					"master.identity.org-events": {
						1: 0,
					},
				},
			},
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			captured := map[string]map[string]map[int32]int64{}
			observer := func(groupID, topic string, partition int32, lag int64) {
				topics, ok := captured[groupID]
				if !ok {
					topics = map[string]map[int32]int64{}
					captured[groupID] = topics
				}

				partitions, ok := topics[topic]
				if !ok {
					partitions = map[int32]int64{}
					topics[topic] = partitions
				}

				partitions[partition] = lag
			}

			observeLag(ObserverFunc(observer), tc.Topics, tc.Groups)
			assert.Equal(t, tc.Lag, captured)
		})
	}
}
