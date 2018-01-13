package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/savaki/kag"
	"github.com/savaki/kag/datadog"
	"gopkg.in/urfave/cli.v1"
)

var (
	opts = struct {
		Brokers  string
		Observer string
		Interval time.Duration
		Datadog  struct {
			Addr      string
			Namespace string
			Tags      string
		}
	}{}
)

func main() {
	app := cli.NewApp()
	app.Action = run
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "brokers",
			Value:       "localhost:9092",
			Usage:       "comma separated list of brokers e.g. localhost:9092",
			EnvVar:      "KAG_BROKERS",
			Destination: &opts.Brokers,
		},
		cli.DurationFlag{
			Name:        "interval",
			Value:       time.Minute,
			Usage:       "interval between polling",
			EnvVar:      "KAG_INTERVAL",
			Destination: &opts.Interval,
		},
		cli.StringFlag{
			Name:        "observer",
			Value:       "stdout",
			Usage:       "observer for stdout; stdout, datadog",
			EnvVar:      "KAG_OBSERVER",
			Destination: &opts.Observer,
		},
		cli.StringFlag{
			Name:        "datadog-addr",
			Value:       "127.0.0.1:8125",
			Usage:       "statsd host and port; require --observer datadog",
			EnvVar:      "KAG_DATADOG_ADDR",
			Destination: &opts.Datadog.Addr,
		},
		cli.StringFlag{
			Name:        "datadog-namespace",
			Usage:       "optional datadog namespace",
			EnvVar:      "KAG_DATADOG_NAMESPACE",
			Destination: &opts.Datadog.Namespace,
		},
		cli.StringFlag{
			Name:        "datadog-tags",
			Usage:       "comma separated list of datadog tags",
			EnvVar:      "KAG_DATADOG_TAGS",
			Destination: &opts.Datadog.Tags,
		},
	}
	app.Run(os.Args)
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newObserver() (kag.Observer, error) {
	observer := kag.Nop

	switch opts.Observer {
	case "stdout":
		observer = kag.Stdout

	case "datadog":
		tags := strings.Split(opts.Datadog.Tags, ",")
		return datadog.NewObserver(opts.Datadog.Addr, opts.Datadog.Namespace, tags...)

	default:
		return nil, fmt.Errorf("unknown observer, %v.  valid observers stdout, datadog", opts.Observer)
	}

	return observer, nil
}

func run(_ *cli.Context) error {
	observer, err := newObserver()
	check(err)

	monitor := kag.New(kag.Config{
		Brokers:  strings.Split(opts.Brokers, ","),
		Observer: observer,
		Interval: opts.Interval,
	})
	defer monitor.Close()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Kill, os.Interrupt)

	<-stop

	if closer, ok := observer.(io.Closer); ok {
		closer.Close()
	}

	return nil
}
