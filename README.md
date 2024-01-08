# Java Spring Kafka

Kafka contains pre-populated messages distributed among partitions \
Application will consumer message right after startup `->` see logs


## Description
- `OrderListener` simulates long-running operation - 2000millis for one message, but topic has 3 partitions and listener has `concurrency = "3"` so we parallelize processing.
- `PaymentListener` simulates failure using custom exceptions Retryable and Non-Retryable 

## Run
```shell
docker-compose up -d
```
```shell
 ./gradlew bootRun
```


## Test

[Open Kafka-UI](http://localhost:9000)
