package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
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
		Debug    bool
		ECS      bool
		Datadog  struct {
			Addr      string
			Namespace string
			Tags      string
		}
		TLS struct {
			Cert string
			Key  string
			CA   string
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
		cli.StringFlag{
			Name:        "tls-cert",
			Usage:       "tls certificate",
			EnvVar:      "KAG_TLS_CERT",
			Destination: &opts.TLS.Cert,
		},
		cli.StringFlag{
			Name:        "tls-key",
			Usage:       "tls private key",
			EnvVar:      "KAG_TLS_KEY",
			Destination: &opts.TLS.Key,
		},
		cli.StringFlag{
			Name:        "tls-ca",
			Usage:       "tls ca certificate",
			EnvVar:      "KAG_TLS_CA",
			Destination: &opts.TLS.CA,
		},
		cli.BoolFlag{
			Name:        "debug",
			Usage:       "display additional debugging info",
			Destination: &opts.Debug,
		},
		cli.BoolFlag{
			Name:        "ecs",
			Usage:       "use the address of the ecs host",
			Destination: &opts.ECS,
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

func ecsHost() (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	u := "http://169.254.169.254/latest/meta-data/local-ipv4"
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to instance metadata host, %v: %v\n", u, err)
		return "", false
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read host info from instance metadata: %v\n", err)
		return "", false
	}

	return strings.TrimSpace(string(data)), true
}

func newObserver() (kag.Observer, error) {
	observer := kag.Nop

	switch opts.Observer {
	case "stdout":
		observer = kag.Stdout

	case "datadog":
		addr := opts.Datadog.Addr
		if opts.ECS {
			if host, ok := ecsHost(); ok {
				addr = fmt.Sprintf("%v:8125", host)
			}
		}
		tags := strings.Split(opts.Datadog.Tags, ",")
		return datadog.NewObserver(addr, opts.Datadog.Namespace, tags...)

	default:
		return nil, fmt.Errorf("unknown observer, %v.  valid observers stdout, datadog", opts.Observer)
	}

	return observer, nil
}

func lookupTlsConfig() (*tls.Config, error) {
	if opts.TLS.Cert == "" || opts.TLS.Key == "" || opts.TLS.CA == "" {
		return nil, nil
	}

	cert, err := tls.X509KeyPair([]byte(opts.TLS.Cert), []byte(opts.TLS.Key))
	if err != nil {
		return nil, fmt.Errorf("unable to read x509 key pair: %v", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(opts.TLS.CA))

	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: true,
	}, nil
}

func run(_ *cli.Context) error {
	observer, err := newObserver()
	check(err)

	tlsConfig, err := lookupTlsConfig()
	check(err)

	var w io.Writer
	if opts.Debug {
		w = os.Stdout
	}

	monitor := kag.New(kag.Config{
		Brokers:  strings.Split(opts.Brokers, ","),
		Observer: observer,
		Interval: opts.Interval,
		TLS:      tlsConfig,
		Debug:    w,
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
