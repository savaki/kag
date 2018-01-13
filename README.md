# kag

kafka lag meter

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
   --debug                    display additional debugging info
   --ecs                      use the address of the ecs host
   --help, -h                 show help
   --version, -v              print the version
```

### Datadog

```kag``` has a built in datadog metrics publisher

```bash
kag --observer datadog 
```