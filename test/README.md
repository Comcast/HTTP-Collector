# Smoketest for http-collector (HC)
This is a smoketest for HC.   

### Command-line arguments:
To run the smoketest, provide the following parameters:
- kafkaBrokers - bootstrap servers to Kafka
- KafakVersion - Kafka version (default 2.1.0.0)
- kafkaTopic - The topic to POST the messages HC
- testAgent - Value for k8s-test header (default canary-test) (prod-test is the other)
- url - URL of HC (up to the / before the topic, full - URL will be <url><kafkaTopic>)
- timeout - Maximum time allowed to complete the test (default 10m)
    
`./test -kafkaBrokers <kafkabroker>>:9092 -kafkaTopic k8s_test_topic -url <host>:8091/v2/nolim/
`

### Exit status:
```
0 Smoketest passed
>0 Smoketest failed
```
