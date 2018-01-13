package datadog

import (
	"fmt"
	"os"
	"strconv"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/pkg/errors"
)

type Observer struct {
	client *statsd.Client
}

func (o *Observer) Observe(groupID, topic string, partition int32, lag int64) {
	tags := []string{
		"group:" + groupID,
		"topic:" + topic,
		"partition:" + strconv.Itoa(int(partition)),
	}
	if err := o.client.Gauge("kafka.consumer.lag", float64(lag), tags, 1); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func (o *Observer) Flush() error {
	return o.client.Flush()
}

func (o *Observer) Close() error {
	o.Flush()
	return o.client.Close()
}

func NewObserver(addr, namespace string, tags ...string) (*Observer, error) {
	client, err := statsd.New(addr)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create statsd client")
	}
	client.Namespace = namespace
	client.Tags = tags

	return &Observer{
		client: client,
	}, nil
}
