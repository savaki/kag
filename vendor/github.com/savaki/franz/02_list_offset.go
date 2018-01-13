package franz

import "bufio"

type ListOffsetRequestV1 struct {
	ReplicaID int32
	Topics    []ListOffsetRequestV1Topic
}

func (r ListOffsetRequestV1) size() int32 {
	return sizeofInt32(r.ReplicaID) +
		sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
}

func (r ListOffsetRequestV1) writeTo(w *bufio.Writer) {
	writeInt32(w, r.ReplicaID)
	writeArray(w, len(r.Topics), func(i int) { r.Topics[i].writeTo(w) })
}

type ListOffsetRequestV1Topic struct {
	TopicName  string
	Partitions []ListOffsetRequestV1Partition
}

func (t ListOffsetRequestV1Topic) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t ListOffsetRequestV1Topic) writeTo(w *bufio.Writer) {
	writeString(w, t.TopicName)
	writeArray(w, len(t.Partitions), func(i int) { t.Partitions[i].writeTo(w) })
}

type ListOffsetRequestV1Partition struct {
	Partition int32

	// Time is used to ask for all messages before a certain time (ms).
	//
	// There are two special values. Specify -1 to receive the latest offset (i.e.
	// the offset of the next coming message) and -2 to receive the earliest
	// available offset. This applies to all versions of the API. Note that because
	// offsets are pulled in descending order, asking for the earliest offset will
	// always return you a single element.
	Time int64
}

func (p ListOffsetRequestV1Partition) size() int32 {
	return 4 + 8
}

func (p ListOffsetRequestV1Partition) writeTo(w *bufio.Writer) {
	writeInt32(w, p.Partition)
	writeInt64(w, p.Time)
}

type ListOffsetResponseV1 struct {
	Responses []ListOffsetResponseV1Response
}

func (t ListOffsetResponseV1) size() int32 {
	return sizeofArray(len(t.Responses), func(i int) int32 { return t.Responses[i].size() })
}

func (t ListOffsetResponseV1) writeTo(w *bufio.Writer) {
	writeArray(w, len(t.Responses), func(i int) { t.Responses[i].writeTo(w) })
}

func (t *ListOffsetResponseV1) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	fn := func(r *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		var item ListOffsetResponseV1Response
		if fnRemain, fnErr = (&item).readFrom(r, withSize); fnErr != nil {
			return
		}
		t.Responses = append(t.Responses, item)
		return
	}
	if remain, err = readArrayWith(r, sz, fn); err != nil {
		return
	}

	return
}

type ListOffsetResponseV1Response struct {
	Topic              string
	PartitionResponses []ListOffsetResponseV1Partition
}

func (t ListOffsetResponseV1Response) size() int32 {
	return sizeofString(t.Topic) +
		sizeofArray(len(t.PartitionResponses), func(i int) int32 { return t.PartitionResponses[i].size() })
}

func (t ListOffsetResponseV1Response) writeTo(w *bufio.Writer) {
	writeString(w, t.Topic)
	writeArray(w, len(t.PartitionResponses), func(i int) { t.PartitionResponses[i].writeTo(w) })
}

func (t *ListOffsetResponseV1Response) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readString(r, sz, &t.Topic); err != nil {
		return
	}

	fn := func(r *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		var item ListOffsetResponseV1Partition
		if fnRemain, fnErr = (&item).readFrom(r, withSize); fnErr != nil {
			return
		}
		t.PartitionResponses = append(t.PartitionResponses, item)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}

type ListOffsetResponseV1Partition struct {
	Partition int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

func (t ListOffsetResponseV1Partition) size() int32 {
	return sizeofInt32(t.Partition) +
		sizeofInt16(t.ErrorCode) +
		sizeofInt64(t.Timestamp) +
		sizeofInt64(t.Offset)
}

func (t ListOffsetResponseV1Partition) writeTo(w *bufio.Writer) {
	writeInt32(w, t.Partition)
	writeInt16(w, t.ErrorCode)
	writeInt64(w, t.Timestamp)
	writeInt64(w, t.Offset)
}

func (t *ListOffsetResponseV1Partition) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readInt32(r, sz, &t.Partition); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ErrorCode); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &t.Timestamp); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &t.Offset); err != nil {
		return
	}
	return
}
