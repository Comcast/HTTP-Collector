# http-collector-go
A lightweight http collector for Kafka written in Go. 

It is by design to be as lightweight as possible, it should do 1 thing and only 1 thing very well, which is to expose a REST API for others to send content into Kafka. To keep it high performant, it batches messages (size and delay are configurable) and send to Kafka via an asynchronous producer. It supports graceful shutdown, which will disable all incoming connections, shutdown web server (allows up to 30s for existing requests to complete), flush and write all messages to Kafka and shutdown Kafka producer.


The API exposes 2 endpoints:  
`/v2/nolim/<topic name>` - no rate limit  
`/v2/limit/<topic name>` - enforces rate limit, configurable via Limit  

Example: `/v2/nolim/testTopic`
* Topic name is obtained via the path. Using the above example, the topic name would be testTopic.
* Partition key is obtained via header: `X-key-prefix` and `X-key`
* Value is the entire body.

Partition key:  
If both `X-key-prefix` and `X-key` are present: `<X-key-prefix>-<X-key>`  
If `X-key-prefix` is present and `X-key` is not: `<X-key-prefix>-<random num>`  
If `X-key-prefix` is not present and `X-key` is: `<X-key>`  
If neither `X-key-prefix` nor `X-key` are present: random partition will be picked

Config can be via (highest priority to lowest priority):
1. Commandline (limited to listening addr, Kafka brokers, config filename and verbose)
2. Environmental variables (limited to HC_ADDR, KAFKA_BROKERS, HC_CONFIG)
3. Config file

Configuration file (see config.json.template for an example):  
Complete Kafka config options can be found at: https://godoc.org/github.com/Shopify/sarama#Config  
Web config options can be found at: https://golang.org/pkg/net/http/#Server  
Redis config options can be found at: https://godoc.org/github.com/go-redis/redis#UniversalOptions  
Limit sets how many request allowed per `X-key-prefix` and `X-key` per day  

Metrics API is at `/metrics`  
