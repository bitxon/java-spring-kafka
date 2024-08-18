# Java Spring Kafka

This is a playground for Spring & Kafka

---
## Kafka: Single Pojo Processing

### Description
- `OrderListener` simulates long-running operation - 2000millis for one message, but topic has 3 partitions and listener has `concurrency = "3"` so we parallelize processing.
- `PaymentListener` simulates failure using custom exceptions Retryable and Non-Retryable

### Run
```shell
docker compose up -d
```
```shell
./gradlew kafka-single-pojo:bootRun
```

---
## Kafka: Batch Pojo (v1) Processing

### Description
- `ShipmentBatchListener` simulates failure using custom exceptions Retryable and Non-Retryable in **Batch**

### Run
```shell
docker compose up -d
```
```shell
./gradlew kafka-batch-pojo:bootRun
```

---
## Kafka: Batch Pojo (v2) Processing
This module has the same logic as v1 but configuration is done without Spring auto-configurations

### Description
- `ShipmentBatchListener` simulates failure using custom exceptions Retryable and Non-Retryable in **Batch**

### Run
```shell
docker compose up -d
```
```shell
./gradlew kafka-batch-pojo-v2:bootRun
```

---
## Test

[Open Kafka-UI](http://localhost:9000)
