# kag

kafka lag meter written in pure go with no external dependencies (except Kafka)

### Motivations

We use Kafka extensively, but unlike many organizations, our Kafka clustered are managed
by a hosting provider.  Consequently, when we looked for kafka monitoring tools, we found
a derth of options:

* **Burrow** looks like a fantastic tool, but appears to require a connection to zookeeper 
which our hosting provider doesn't expose
* **Datadog** provides an agent to extract stats from Kafka via JMX.  Again, with a hosted 
Kafka, we don't have JMX access.  Also, it appears that the agent should be colocated with the
Kafka instances. 

### Installation

```bash
go get github.com/savaki/kag/cmd/kag
```

### Usage

```bash
NAME:
   kag - A new cli application

USAGE:
   main [global options] command [command options] [arguments...]

VERSION:
   0.0.0

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --brokers value            comma separated list of brokers e.g. localhost:9092 (default: "localhost:9092") [$KAG_BROKERS]
   --interval value           interval between polling (default: 1m0s) [$KAG_INTERVAL]
   --observer value           observer for stdout; stdout, datadog (default: "stdout") [$KAG_OBSERVER]
   --datadog-addr value       statsd host and port; require --observer datadog (default: "127.0.0.1:8125") [$KAG_DATADOG_ADDR]
   --datadog-namespace value  optional datadog namespace [$KAG_DATADOG_NAMESPACE]
   --datadog-tags value       comma separated list of datadog tags [$KAG_DATADOG_TAGS]
   --tls-cert value           tls certificate [$KAG_TLS_CERT]
   --tls-key value            tls private key [$KAG_TLS_KEY]
   --tls-ca value             tls ca certificate [$KAG_TLS_CA]
   --debug                    display additional debugging info [$KAG_DEBUG]
   --ecs                      use the address of the ecs host [$KAG_ECS]
   --help, -h                 show help
   --version, -v              print the version
```

### Datadog

```kag``` has a built in datadog metrics publisher

```bash
kag --observer datadog 
```

### Configuration

kag can be configured entirely from environment variables

| Name | Default Value | Description |
| :--- | :--- | :--- |
| KAG_BROKERS | localhost:9092 | comma separated list of kafka brokers |
| KAG_INTERVAL | 1m | polling interval. examples 5m, 90s, 1h  |
| KAG_OBSERVER | stdout | indicates where metrics should be published; stdout, datadog |
| KAG_DATADOG_ADDR | 127.0.0.1:8125 | statsd host and port when using datadog observer |
| KAG_DATADOG_NAMESPACE | | optional datadog namespace |
| KAG_DATADOG_TAGS | | comma separated list of datadog tags |
| KAG_DEBUG | | true to include additional debug data |
| KAG_ECS | | true to use the AWS ECS host as the base address for the observer e.g. for datadog {host}:8125 |
| KAG_TLS_CERT | | optional tls cert pem |
| KAG_TLS_KEY | | optional tls private key pem for cert |
| KAG_TLS_CA | | optional tls ca certification |
